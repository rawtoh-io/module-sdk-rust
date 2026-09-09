// One enrolled instance's connection to the hub: challenge/response
// registration, reconnect with exponential backoff, and the subscription
// table `event.subscribe` fills in.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::watch;

use crate::error::{Error, RpcError};
use crate::identity::{Identity, sign_challenge};
use crate::ws::{Dispatcher, WsClient};

// Close codes sent by the hub. Mirrors `apps/rpc/src/close-codes.ts`.
/// The user asked to disconnect this module. Do not reconnect.
pub const CLOSE_DISCONNECT_REQUESTED: u16 = 4000;
/// The instance was re-enrolled against another key pair. Do not reconnect.
pub const CLOSE_KEY_ROTATED: u16 = 4001;

const MAX_BACKOFF_SECS: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    DisconnectRequested,
    KeyRotated,
}

/// Connection state, observable through [`HubConnection::status`].
/// `reason` is set when the hub refused reconnection and the loop has stopped for good.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Status {
    pub connected: bool,
    pub reason: Option<DisconnectReason>,
}

pub type MethodFuture<T = Result<Value, RpcError>> = Pin<Box<dyn Future<Output = T> + Send>>;

/// The module's side of the protocol. `ping`, `event.subscribe` and
/// `event.unsubscribe` are handled by the SDK; everything else lands in `call`.
pub trait Handler: Send + Sync + 'static {
    /// A module method (`chat.say`, ...). Params are whatever the hub sent
    /// (an object for by-name methods).
    fn call(&self, method: &str, params: Value) -> MethodFuture;

    /// The hub wants `event`. Return false for an unknown event, or when the
    /// upstream subscription (e.g. Twitch EventSub) could not be set up.
    fn subscribe(&self, _event: &str) -> MethodFuture<bool> {
        Box::pin(async { true })
    }

    /// No subscription references `event` any more.
    fn unsubscribe(&self, _event: &str) -> MethodFuture<()> {
        Box::pin(async {})
    }
}

pub struct HubOptions {
    /// WebSocket URL of the hub (`RAWTOH_WS_URL`).
    pub url: String,
    pub identity: Identity,
    /// Shown in log lines, e.g. the account name.
    pub label: Option<String>,
}

struct Inner {
    opts: HubOptions,
    handler: Arc<dyn Handler>,
    /// Subscription id → event name, as returned to the hub on `event.subscribe`.
    subscriptions: Mutex<HashMap<String, String>>,
    client: Mutex<Option<WsClient>>,
    stopping: AtomicBool,
    status: watch::Sender<Status>,
}

#[derive(Clone)]
pub struct HubConnection {
    inner: Arc<Inner>,
}

impl HubConnection {
    pub fn new(opts: HubOptions, handler: impl Handler) -> Self {
        let (status, _) = watch::channel(Status::default());
        Self {
            inner: Arc::new(Inner {
                opts,
                handler: Arc::new(handler),
                subscriptions: Mutex::new(HashMap::new()),
                client: Mutex::new(None),
                stopping: AtomicBool::new(false),
                status,
            }),
        }
    }

    pub fn connected(&self) -> bool {
        self.inner.status.borrow().connected
    }

    /// Watch connection changes; `wait_for(|s| s.reason.is_some())` is the
    /// "gave up for good" signal.
    pub fn status(&self) -> watch::Receiver<Status> {
        self.inner.status.subscribe()
    }

    /// The first attempt runs in-band so callers get immediate feedback; the
    /// reconnect loop then continues in the background.
    pub async fn start(&self) -> Result<(), Error> {
        self.inner.stopping.store(false, Ordering::SeqCst);
        self.inner.connect_once().await?;
        self.inner.set_status(true, None);
        let inner = self.inner.clone();
        tokio::spawn(async move { inner.run().await });
        Ok(())
    }

    /// Close the socket and stop reconnecting.
    pub fn stop(&self) {
        self.inner.stopping.store(true, Ordering::SeqCst);
        if let Some(client) = self.inner.client.lock().unwrap().take() {
            client.terminate();
        }
        self.inner.set_status(false, None);
    }

    /// Push an event to the hub for every subscription on `event`.
    /// `emitted_by` is the rawtoh user id behind the event, when a human is in the loop.
    pub fn emit(&self, event: &str, result: Value, emitted_by: Option<&str>) {
        let Some(client) = self.inner.client.lock().unwrap().clone() else {
            return;
        };
        let subs = self.inner.subscriptions.lock().unwrap();
        for (subscription, _) in subs.iter().filter(|(_, n)| *n == event) {
            let mut params = json!({ "subscription": subscription, "result": result });
            if let Some(user) = emitted_by {
                params["emitted_by"] = json!(user);
            }
            client.notify("event.subscription", params);
        }
    }
}

impl Inner {
    fn log(&self, msg: &str) {
        match &self.opts.label {
            Some(label) => log::info!("[ws:{label}] {msg}"),
            None => log::info!("[ws] {msg}"),
        }
    }

    fn set_status(&self, connected: bool, reason: Option<DisconnectReason>) {
        self.status.send_replace(Status { connected, reason });
    }

    fn dispatcher(self: &Arc<Self>) -> Dispatcher {
        let weak = Arc::downgrade(self);
        Arc::new(move |method, params| {
            let weak = weak.clone();
            Box::pin(async move {
                match weak.upgrade() {
                    Some(inner) => inner.dispatch(&method, params).await,
                    None => Err(RpcError::internal("connection dropped")),
                }
            })
        })
    }

    async fn dispatch(self: Arc<Self>, method: &str, params: Value) -> Result<Value, RpcError> {
        match method {
            "ping" => Ok(json!({ "pong": true })),
            "event.subscribe" => {
                // Params are positional: [event_name, params?].
                let Some(name) = params.get(0).and_then(Value::as_str) else {
                    return Ok(Value::Null);
                };
                if self
                    .subscriptions
                    .lock()
                    .unwrap()
                    .values()
                    .any(|n| n == name)
                {
                    return Ok(Value::Null);
                }
                if !self.handler.subscribe(name).await {
                    return Ok(Value::Null);
                }
                let id = random_id();
                self.subscriptions
                    .lock()
                    .unwrap()
                    .insert(id.clone(), name.to_owned());
                self.log(&format!("Subscribed to \"{name}\" -> {id}"));
                Ok(Value::String(id))
            }
            "event.unsubscribe" => {
                if let Some(id) = params.get(0).and_then(Value::as_str) {
                    let removed = self.subscriptions.lock().unwrap().remove(id);
                    if let Some(name) = removed {
                        let still_used = self
                            .subscriptions
                            .lock()
                            .unwrap()
                            .values()
                            .any(|n| *n == name);
                        if !still_used {
                            self.handler.unsubscribe(&name).await;
                        }
                        self.log(&format!("Unsubscribed {id}"));
                    }
                }
                Ok(Value::Bool(true))
            }
            _ => self.handler.call(method, params).await,
        }
    }

    async fn connect_once(self: &Arc<Self>) -> Result<(), Error> {
        let identity = &self.opts.identity;
        self.log(&format!("Connecting to {}...", self.opts.url));
        let client = WsClient::connect(&self.opts.url, self.dispatcher()).await?;

        // The hub issues a nonce, we sign it with the key generated at enrollment.
        // Both calls must land inside the hub's 5s registration window.
        let challenge = client
            .request(
                "session.challenge",
                json!({ "instance_id": identity.instance_id }),
            )
            .await?;
        let Some(nonce) = challenge.get("nonce").and_then(Value::as_str) else {
            client.close();
            return Err(Error::Protocol("Hub returned no challenge"));
        };

        let registered = client
            .request(
                "session.register",
                json!({
                    "instance_id": identity.instance_id,
                    "signature": sign_challenge(identity, nonce)?,
                }),
            )
            .await?;
        if registered != Value::Bool(true) {
            client.close();
            return Err(Error::Protocol("Registration rejected"));
        }

        self.log("Registered successfully");
        *self.client.lock().unwrap() = Some(client);
        Ok(())
    }

    async fn run(self: Arc<Self>) {
        let mut delay = 1u64;
        while !self.stopping.load(Ordering::SeqCst) {
            let Some(client) = self.client.lock().unwrap().clone() else {
                return;
            };

            let code = client.wait_closed().await;
            self.log(&format!("Disconnected (code: {code:?})"));
            *self.client.lock().unwrap() = None;
            self.subscriptions.lock().unwrap().clear();

            let reason = match code {
                Some(CLOSE_KEY_ROTATED) => Some(DisconnectReason::KeyRotated),
                Some(CLOSE_DISCONNECT_REQUESTED) => Some(DisconnectReason::DisconnectRequested),
                _ => None,
            };
            if reason.is_some() {
                self.log(&format!("Close code {code:?} — not reconnecting"));
            }
            if !self.stopping.load(Ordering::SeqCst) {
                self.set_status(false, reason);
            }
            if reason.is_some() {
                return;
            }

            while !self.stopping.load(Ordering::SeqCst) {
                self.log(&format!("Reconnecting in {delay}s..."));
                tokio::time::sleep(Duration::from_secs(delay)).await;
                delay = (delay * 2).min(MAX_BACKOFF_SECS);
                if self.stopping.load(Ordering::SeqCst) {
                    return;
                }
                match self.connect_once().await {
                    Ok(()) => {
                        delay = 1;
                        self.set_status(true, None);
                        break;
                    }
                    Err(err) => log::error!(
                        "[ws:{}] Connection error: {err}",
                        self.opts.label.as_deref().unwrap_or("")
                    ),
                }
            }
        }
    }
}

fn random_id() -> String {
    let mut bytes = [0u8; 16];
    // getrandom only fails on a broken platform RNG; a zeroed id still works as a key.
    let _ = getrandom::fill(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
