// One JSON-RPC 2.0 session over a WebSocket to the hub, with a ping watchdog.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::error::{Error, RpcError};

/// WebSocket close code, `None` when the socket died without a close frame.
pub type CloseCode = Option<u16>;

/// Answers the hub's inbound requests: `(method, params) -> result`.
pub type Dispatcher = Arc<
    dyn Fn(String, Value) -> Pin<Box<dyn Future<Output = Result<Value, RpcError>> + Send>>
        + Send
        + Sync,
>;

const PING_EVERY: Duration = Duration::from_secs(10);
const PING_TIMEOUT: Duration = Duration::from_secs(5);

struct Shared {
    tx: mpsc::UnboundedSender<Message>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>,
    next_id: AtomicU64,
    // `None` while open, `Some(code)` once closed.
    closed: watch::Sender<Option<CloseCode>>,
}

impl Shared {
    fn send(&self, value: Value) {
        // A dead socket is caught by the ping watchdog; nothing to do here.
        let _ = self.tx.send(Message::text(value.to_string()));
    }

    fn mark_closed(&self, code: CloseCode) {
        self.closed.send_if_modified(|c| {
            if c.is_some() {
                return false;
            }
            *c = Some(code);
            true
        });
        let pending = std::mem::take(&mut *self.pending.lock().unwrap());
        for (_, tx) in pending {
            let _ = tx.send(Err(RpcError::internal("WebSocket closed")));
        }
    }
}

/// Resolves once the socket is closed. Copies the code out so the future is
/// `Send` (a `watch::Ref` is not).
async fn wait_closed(rx: &mut watch::Receiver<Option<CloseCode>>) -> CloseCode {
    rx.wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|v| *v)
        .flatten()
}

#[derive(Clone)]
pub struct WsClient {
    shared: Arc<Shared>,
}

impl WsClient {
    /// Open the socket and start the reader, writer and ping tasks.
    /// `dispatcher` serves the hub's requests (`ping`, `event.subscribe`, module methods).
    pub async fn connect(url: &str, dispatcher: Dispatcher) -> Result<Self, Error> {
        let (ws, _) = connect_async(url).await?;
        let (mut sink, mut stream) = ws.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
        let (closed, _) = watch::channel(None);
        let shared = Arc::new(Shared {
            tx,
            pending: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            closed,
        });
        let client = Self {
            shared: shared.clone(),
        };

        // Writer: drains the outbound queue until the socket closes.
        let mut closed_rx = shared.closed.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = rx.recv() => match msg {
                        Some(msg) => if sink.send(msg).await.is_err() { break },
                        None => break,
                    },
                    _ = wait_closed(&mut closed_rx) => break,
                }
            }
            let _ = sink.close().await;
        });

        // Reader: routes responses to waiters and requests to the dispatcher.
        let reader = client.clone();
        tokio::spawn(async move {
            let code = loop {
                match stream.next().await {
                    Some(Ok(Message::Text(text))) => reader.incoming(&text, &dispatcher),
                    Some(Ok(Message::Close(frame))) => break frame.map(|f| u16::from(f.code)),
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => break None,
                }
            };
            reader.shared.mark_closed(code);
        });

        // Ping watchdog: the hub answers "pong"; anything else means a dead link.
        let pinger = client.clone();
        tokio::spawn(async move {
            let mut closed_rx = pinger.shared.closed.subscribe();
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(PING_EVERY) => {}
                    _ = wait_closed(&mut closed_rx) => return,
                }
                let ok = matches!(
                    tokio::time::timeout(PING_TIMEOUT, pinger.request("ping", json!({}))).await,
                    Ok(Ok(Value::String(s))) if s == "pong"
                );
                if !ok {
                    log::warn!("[ws] Ping failed, closing");
                    pinger.terminate();
                    return;
                }
            }
        });

        Ok(client)
    }

    fn incoming(&self, text: &str, dispatcher: &Dispatcher) {
        let Ok(msg) = serde_json::from_str::<Value>(text) else {
            return;
        };

        if let Some(method) = msg.get("method").and_then(Value::as_str) {
            let id = msg.get("id").filter(|id| !id.is_null()).cloned();
            let params = msg.get("params").cloned().unwrap_or(Value::Null);
            let fut = dispatcher(method.to_owned(), params);
            let shared = self.shared.clone();
            tokio::spawn(async move {
                let result = fut.await;
                // Notifications (no id) get no reply.
                let Some(id) = id else { return };
                shared.send(match result {
                    Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
                    Err(error) => json!({ "jsonrpc": "2.0", "id": id, "error": error }),
                });
            });
            return;
        }

        let Some(id) = msg.get("id").and_then(Value::as_u64) else {
            return;
        };
        let Some(tx) = self.shared.pending.lock().unwrap().remove(&id) else {
            return;
        };
        let outcome = match msg.get("error") {
            Some(err) => Err(serde_json::from_value(err.clone())
                .unwrap_or_else(|_| RpcError::internal(err.to_string()))),
            None => Ok(msg.get("result").cloned().unwrap_or(Value::Null)),
        };
        let _ = tx.send(outcome);
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value, Error> {
        if self.shared.closed.borrow().is_some() {
            return Err(Error::Closed);
        }
        let id = self.shared.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.shared.pending.lock().unwrap().insert(id, tx);
        self.shared
            .send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        match rx.await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(err)) if err.message == "WebSocket closed" => Err(Error::Closed),
            Ok(Err(err)) => Err(err.into()),
            Err(_) => Err(Error::Closed),
        }
    }

    pub fn notify(&self, method: &str, params: Value) {
        self.shared
            .send(json!({ "jsonrpc": "2.0", "method": method, "params": params }));
    }

    /// Resolves with the close code once the socket is gone.
    pub async fn wait_closed(&self) -> CloseCode {
        wait_closed(&mut self.shared.closed.subscribe()).await
    }

    /// Polite close: send a close frame, let the peer finish.
    pub fn close(&self) {
        let _ = self.shared.tx.send(Message::Close(None));
    }

    /// Drop the socket now.
    pub fn terminate(&self) {
        self.shared.mark_closed(None);
    }
}
