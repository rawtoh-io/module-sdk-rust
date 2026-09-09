// End-to-end check against a fake hub: registration handshake (signature
// verified with the enrolled public key), subscribe phase, module method
// dispatch, and event fan-out. Fails if any wire-format detail drifts.

use ed25519_dalek::{Signature, SigningKey, Verifier};
use futures_util::{SinkExt, StreamExt};
use rawtoh_module_sdk::{Handler, HubConnection, HubOptions, Identity, MethodFuture};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

struct Echo;
impl Handler for Echo {
    fn call(&self, method: &str, params: Value) -> MethodFuture {
        let method = method.to_owned();
        Box::pin(async move { Ok(json!({ "echo": method, "params": params })) })
    }
    fn subscribe(&self, event: &str) -> MethodFuture<bool> {
        let known = event == "chat.message";
        Box::pin(async move { known })
    }
}

async fn recv<S>(ws: &mut S) -> Value
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str::<Value>(&t).unwrap();
        }
    }
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[tokio::test]
async fn registers_subscribes_and_emits() {
    let seed = [42u8; 32];
    let public = SigningKey::from_bytes(&seed).verifying_key();
    let identity = Identity {
        instance_id: "inst_1".into(),
        private_key: b64(&seed),
    };

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());

    // Fake hub: one scripted session.
    let hub = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let challenge = recv(&mut ws).await;
        assert_eq!(challenge["method"], "session.challenge");
        assert_eq!(challenge["params"]["instance_id"], "inst_1");
        ws.send(Message::text(
            json!({ "jsonrpc": "2.0", "id": challenge["id"], "result": { "nonce": "n0nce" } })
                .to_string(),
        ))
        .await
        .unwrap();

        let register = recv(&mut ws).await;
        assert_eq!(register["method"], "session.register");
        let sig = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            register["params"]["signature"].as_str().unwrap(),
        )
        .unwrap();
        public
            .verify(
                b"rawtoh-module-register:v1\ninst_1\nn0nce",
                &Signature::from_slice(&sig).unwrap(),
            )
            .expect("signature over the v1 format");
        ws.send(Message::text(
            json!({ "jsonrpc": "2.0", "id": register["id"], "result": true }).to_string(),
        ))
        .await
        .unwrap();

        // Subscribe phase: positional params, unknown event → null.
        ws.send(Message::text(
            json!({ "jsonrpc": "2.0", "id": 100, "method": "event.subscribe", "params": ["chat.message"] }).to_string(),
        ))
        .await
        .unwrap();
        ws.send(Message::text(
            json!({ "jsonrpc": "2.0", "id": 101, "method": "event.subscribe", "params": ["nope"] })
                .to_string(),
        ))
        .await
        .unwrap();
        ws.send(Message::text(
            json!({ "jsonrpc": "2.0", "id": 102, "method": "chat.say", "params": { "message": "hi" } }).to_string(),
        ))
        .await
        .unwrap();

        let mut sub_id = None;
        let mut answered = 0;
        while answered < 3 {
            let msg = recv(&mut ws).await;
            match msg["id"].as_u64() {
                Some(100) => sub_id = Some(msg["result"].as_str().unwrap().to_owned()),
                Some(101) => assert!(msg["result"].is_null()),
                Some(102) => assert_eq!(msg["result"]["params"]["message"], "hi"),
                _ => continue, // the module's own ping
            }
            answered += 1;
        }

        // Event fan-out: a notification (no id) carrying the subscription id.
        let event = loop {
            let msg = recv(&mut ws).await;
            if msg["method"] == "event.subscription" {
                break msg;
            }
        };
        assert!(event.get("id").is_none());
        assert_eq!(event["params"]["subscription"], sub_id.unwrap());
        assert_eq!(event["params"]["result"]["text"], "hello");
        assert_eq!(event["params"]["emitted_by"], "user_9");
    });

    let conn = HubConnection::new(
        HubOptions {
            url,
            identity,
            label: None,
        },
        Echo,
    );
    conn.start().await.expect("handshake");
    assert!(conn.connected());

    // Give the hub's requests time to be dispatched before emitting.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    conn.emit("chat.message", json!({ "text": "hello" }), Some("user_9"));

    tokio::time::timeout(std::time::Duration::from_secs(5), hub)
        .await
        .unwrap()
        .unwrap();
    conn.stop();
}
