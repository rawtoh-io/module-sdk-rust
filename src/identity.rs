// Ed25519 enrollment and challenge signing.
//
// A module generates its own key pair and never sends the private half
// anywhere. Enrollment binds the public half to a Rawtoh instance once; from
// then on, connecting means signing a nonce the hub issues.
//
// The signed byte format is a protocol contract with the hub
// (`packages/module-auth` there). Changing it here alone locks every module out.

use ed25519_dalek::{Signer, SigningKey};
use serde::{Deserialize, Serialize};

use crate::base64url;
use crate::error::Error;

const CHALLENGE_CONTEXT: &str = "rawtoh-module-register:v1";

/// What a module keeps per enrolled instance. Serialize it to a file with
/// mode 600 — it is the whole credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub instance_id: String,
    /// Ed25519 seed (32 bytes), base64url. Never leaves the module.
    /// (The TypeScript SDK stores PKCS#8 instead; the two files are not
    /// interchangeable, which never matters — an identity is per deployment.)
    pub private_key: String,
}

#[derive(Debug, Clone)]
pub struct EnrollResult {
    pub identity: Identity,
    pub instance_name: String,
    pub organization_id: String,
    pub module_slug: String,
}

#[derive(Deserialize)]
struct EnrollResponse {
    instance_id: String,
    instance_name: String,
    organization_id: String,
    module_slug: String,
}

/// Redeem an enrollment token: generate a key pair, hand the hub the public
/// half, keep the private half. The token is spent by the time this returns —
/// on failure the caller needs a fresh one rather than a retry.
pub async fn enroll(api_url: &str, enrollment_token: &str) -> Result<EnrollResult, Error> {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed).map_err(|e| Error::Identity(e.to_string()))?;
    let key = SigningKey::from_bytes(&seed);
    let public_key = base64url::encode(key.verifying_key().as_bytes());

    let res = reqwest::Client::new()
        .post(format!(
            "{}/api/module-enroll",
            api_url.trim_end_matches('/')
        ))
        .json(&serde_json::json!({
            "token": enrollment_token.trim(),
            "public_key": public_key,
        }))
        .send()
        .await?;

    if !res.status().is_success() {
        let status = res.status();
        let body: serde_json::Value = res.json().await.unwrap_or_default();
        let msg = body
            .get("error")
            .and_then(|e| e.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("Enrollment failed ({status})"));
        return Err(Error::Enroll(msg));
    }

    let data: EnrollResponse = res.json().await?;
    Ok(EnrollResult {
        identity: Identity {
            instance_id: data.instance_id,
            private_key: base64url::encode(&seed),
        },
        instance_name: data.instance_name,
        organization_id: data.organization_id,
        module_slug: data.module_slug,
    })
}

/// Sign a `session.challenge` nonce with the instance's private key.
pub fn sign_challenge(identity: &Identity, nonce: &str) -> Result<String, Error> {
    let seed: [u8; 32] = base64url::decode(&identity.private_key)
        .ok()
        .and_then(|b| b.try_into().ok())
        .ok_or_else(|| Error::Identity("private_key is not a 32-byte base64url seed".into()))?;
    let message = format!("{CHALLENGE_CONTEXT}\n{}\n{nonce}", identity.instance_id);
    let signature = SigningKey::from_bytes(&seed).sign(message.as_bytes());
    Ok(base64url::encode(&signature.to_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier};

    // The signed bytes are the contract with the hub's `packages/module-auth`.
    #[test]
    fn signature_verifies_over_the_v1_format() {
        let seed = [7u8; 32];
        let identity = Identity {
            instance_id: "inst_123".into(),
            private_key: base64url::encode(&seed),
        };
        let public = SigningKey::from_bytes(&seed).verifying_key();

        let sig = base64url::decode(&sign_challenge(&identity, "n0nce").unwrap()).unwrap();
        assert_eq!(sig.len(), 64);
        let sig = Signature::from_slice(&sig).unwrap();

        assert!(
            public
                .verify(b"rawtoh-module-register:v1\ninst_123\nn0nce", &sig)
                .is_ok()
        );
        assert!(
            public
                .verify(b"rawtoh-module-register:v1\ninst_123\nother", &sig)
                .is_err()
        );
    }
}
