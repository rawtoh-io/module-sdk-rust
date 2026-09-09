use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A JSON-RPC 2.0 error object, inbound or outbound.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("rpc error {code}: {message}")]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, format!("Method not found: {method}"))
    }
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(-32602, message)
    }
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(-32603, message)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("websocket: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("{0}")]
    Rpc(#[from] RpcError),
    #[error("enrollment failed: {0}")]
    Enroll(String),
    #[error("invalid identity: {0}")]
    Identity(String),
    #[error("hub protocol: {0}")]
    Protocol(&'static str),
    #[error("connection closed")]
    Closed,
    #[error("request timed out")]
    Timeout,
}
