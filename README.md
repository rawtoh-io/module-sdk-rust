# rawtoh-module-sdk

What a Rust module needs to talk to a Rawtoh hub. Headless counterpart of
`@rawtoh/module-sdk` (TypeScript): same enrollment, same wire protocol, no
user-login half — a Rust module is a CLI or a daemon, enrolled once with a
token from the hub UI (**Module → Instances → Re-enroll**).

```toml
[dependencies]
rawtoh-module-sdk = { git = "https://github.com/rawtoh-io/module-sdk-rust" }
```

```rust
use rawtoh_module_sdk::{enroll, Handler, HubConnection, HubOptions, Identity, MethodFuture};
use serde_json::{json, Value};

struct Twitch;
impl Handler for Twitch {
    fn call(&self, method: &str, params: Value) -> MethodFuture {
        let method = method.to_owned();
        Box::pin(async move {
            match method.as_str() {
                "chat.say" => Ok(json!({ "ok": true })),
                _ => Err(rawtoh_module_sdk::RpcError::method_not_found(&method)),
            }
        })
    }
    fn subscribe(&self, event: &str) -> MethodFuture<bool> {
        let known = event == "chat.message";
        Box::pin(async move { known })
    }
}

// First run: redeem the one-shot token, persist `identity` (mode 600).
let enrolled = enroll("https://app.rawtoh.io", "rth_e_...").await?;
let identity: Identity = enrolled.identity;

let hub = HubConnection::new(
    HubOptions { url: "wss://rpc.rawtoh.io".into(), identity, label: None },
    Twitch,
);
hub.start().await?;                       // challenge/response, then reconnects on its own
hub.emit("chat.message", json!({ "text": "hi" }), None);
```

- **Identity** — `enroll(api_url, token)` redeems an enrollment token into an
  Ed25519 key pair; `sign_challenge` answers `session.challenge`.
- **Hub connection** — `HubConnection` registers, reconnects with backoff
  (1 s → 64 s), keeps the `event.subscribe` table and fans `emit` out to it.
  `ping`, `event.subscribe`, `event.unsubscribe` are served by the SDK; your
  `Handler` gets the module methods. `status()` is a `watch` channel; a
  `reason` means the hub refused reconnection (disconnect requested / key rotated).
- **WsClient** — JSON-RPC 2.0 over WebSocket with a 10 s ping watchdog, if you
  need the raw session.

The protocol is documented in `app/docs/module.md` of the hub repo.

## Check

```bash
cargo test    # unit tests + a scripted fake hub (tests/hub.rs)
```
