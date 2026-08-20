//! The bearer token the Azure Web PubSub data plane authenticates with.
//!
//! HS256 over a two-claim payload, which is all the service requires. Signing a JWT is a
//! dozen lines of well-specified concatenation; the reason it gets its own module is that
//! two of those lines are easy to get wrong in a way that looks like a credential problem
//! rather than a code problem, and both are called out below.

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Mint a bearer token for exactly this request URL.
///
/// Two details produce a clean-looking 401 when they are wrong, and neither is guessable
/// from the error the service returns:
///
///  * **`aud` is the request URL including its query string.** Microsoft's own wording is
///    that it "should be the SAME as your HTTP request url", and `?api-version=...` is part
///    of that. A token is therefore not reusable across two different URLs, which is why
///    this is called per send rather than cached — one HMAC, next to an HTTPS round trip.
///  * **The key is the access key string's UTF-8 bytes, not its base64 decoding.** The
///    access key *looks* like base64 and ends in `=` padding, so decoding it first is the
///    natural mistake; every Azure SDK signs with the raw string.
///
/// `now` is a parameter rather than read here so the tests can assert the claims without a
/// clock, in the same spirit as the rest of this crate's time handling.
pub fn bearer(access_key: &str, audience: &str, now_unix: i64, ttl_secs: i64) -> String {
    // Fixed, so it needs no serialiser. `typ` is optional in RFC 7519 and sent anyway,
    // because every reference implementation sends it.
    const HEADER: &str = r#"{"alg":"HS256","typ":"JWT"}"#;

    // `aud` is a URL we constructed, so it contains no character JSON would need to escape;
    // going through serde_json anyway costs nothing and means that stops being an assumption.
    let payload = serde_json::json!({
        "aud": audience,
        "iat": now_unix,
        "exp": now_unix + ttl_secs,
    })
    .to_string();

    let signing_input = format!("{}.{}", b64(HEADER.as_bytes()), b64(payload.as_bytes()));

    let mut mac = <Hmac<Sha256>>::new_from_slice(access_key.as_bytes())
        .expect("HMAC accepts a key of any length");
    mac.update(signing_input.as_bytes());
    let signature = mac.finalize().into_bytes();

    format!("{signing_input}.{}", b64(&signature))
}

/// base64url without padding, as JWS requires (RFC 7515 §2).
fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_part(part: &str) -> serde_json::Value {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(part)
            .expect("each part is base64url");
        serde_json::from_slice(&bytes).expect("header and payload are JSON")
    }

    const URL: &str = "https://x.webpubsub.azure.com/api/hubs/ddi/groups/sales/:send\
                       ?api-version=2024-12-01";

    #[test]
    fn the_token_has_three_unpadded_base64url_parts() {
        let t = bearer("secret", URL, 1_700_000_000, 300);
        let parts: Vec<&str> = t.split('.').collect();
        assert_eq!(parts.len(), 3, "header.payload.signature: {t}");
        for p in &parts {
            assert!(!p.contains('='), "padding is not allowed in JWS: {p}");
            assert!(
                !p.contains('+') && !p.contains('/'),
                "must be base64url: {p}"
            );
        }
    }

    #[test]
    fn the_audience_is_the_full_request_url_including_the_query() {
        // The api-version query parameter is part of the URL the service compares against,
        // so dropping it yields a 401 that reads exactly like a wrong access key.
        let t = bearer("secret", URL, 1_700_000_000, 300);
        let payload = decode_part(t.split('.').nth(1).unwrap());
        assert_eq!(payload["aud"], URL);
        assert!(
            payload["aud"].as_str().unwrap().contains("api-version"),
            "got: {payload}"
        );
    }

    #[test]
    fn exp_follows_iat_by_the_requested_lifetime() {
        let t = bearer("secret", URL, 1_700_000_000, 300);
        let payload = decode_part(t.split('.').nth(1).unwrap());
        assert_eq!(payload["iat"], 1_700_000_000i64);
        assert_eq!(payload["exp"], 1_700_000_300i64);
    }

    #[test]
    fn the_header_declares_hs256() {
        let t = bearer("secret", URL, 1, 1);
        let header = decode_part(t.split('.').next().unwrap());
        assert_eq!(header["alg"], "HS256");
        assert_eq!(header["typ"], "JWT");
    }

    #[test]
    fn the_key_is_the_access_key_string_not_its_base64_decoding() {
        // A real access key is base64 text ending in '=' padding, which invites decoding it
        // before use. Signing with the decoded bytes produces a token the service rejects,
        // so pin the distinction: the two must not agree.
        let key = "c2VjcmV0LWtleQ==";
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(key)
            .expect("valid base64");
        let signed_with_string = bearer(key, URL, 1, 1);
        let signed_with_decoded = bearer(
            std::str::from_utf8(&decoded).expect("test vector is text"),
            URL,
            1,
            1,
        );
        assert_ne!(
            signed_with_string, signed_with_decoded,
            "the raw string is the key; if these ever agree this test proves nothing"
        );
    }

    #[test]
    fn the_signature_is_hmac_sha256_over_the_signing_input() {
        // Recomputed independently rather than compared against a stored token, so this
        // states the algorithm rather than freezing an output.
        let t = bearer("secret", URL, 42, 60);
        let (signing_input, signature) = t.rsplit_once('.').expect("three parts");
        let mut mac = <Hmac<Sha256>>::new_from_slice(b"secret").unwrap();
        mac.update(signing_input.as_bytes());
        assert_eq!(signature, b64(&mac.finalize().into_bytes()));
    }

    #[test]
    fn a_different_url_yields_a_different_token() {
        // Which is why the token is minted per send rather than cached on the client.
        let a = bearer("secret", URL, 1, 60);
        let b = bearer(
            "secret",
            "https://x.webpubsub.azure.com/api/hubs/ddi/groups/orders/:send\
             ?api-version=2024-12-01",
            1,
            60,
        );
        assert_ne!(a, b);
    }
}
