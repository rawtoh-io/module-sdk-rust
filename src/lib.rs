//! What a Rust module needs to talk to a Rawtoh hub: Ed25519 enrollment,
//! JSON-RPC 2.0 over WebSocket, and a hub connection that registers,
//! reconnects and fans events out to subscriptions.
//!
//! Mirror of `@rawtoh/module-sdk` minus the user-login half: a Rust module is
//! headless (CLI / daemon), enrolled once with a token from the hub UI.

mod base64url;
mod error;
mod hub;
mod identity;
mod ws;

pub use error::{Error, RpcError};
pub use hub::{
    CLOSE_DISCONNECT_REQUESTED, CLOSE_KEY_ROTATED, DisconnectReason, Handler, HubConnection,
    HubOptions, MethodFuture, Status,
};
pub use identity::{EnrollResult, Identity, enroll, sign_challenge};
pub use ws::{CloseCode, WsClient};
