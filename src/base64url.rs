// Same encoding as `@rawtoh/module-auth` on the hub side: URL-safe, no padding.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

pub fn encode(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn decode(value: &str) -> Result<Vec<u8>, base64::DecodeError> {
    URL_SAFE_NO_PAD.decode(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_the_typescript_sdk() {
        // Bytes chosen to exercise `+`/`/` → `-`/`_` and stripped padding.
        assert_eq!(encode(&[0xfb, 0xff, 0xfe]), "-__-");
        assert_eq!(encode(b"a"), "YQ");
        assert_eq!(decode("YQ").unwrap(), b"a");
    }
}
