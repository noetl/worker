//! Worker-side sealed-credential primitives (Secrets Wallet Phase 5c,
//! noetl/ai-meta#61).
//!
//! This is the recipient half of `noetl/server`'s `src/crypto/sealed.rs`.
//! The server-side `seal()` writes a [`SealedEnvelope`] (sender = server,
//! ephemeral key per call); this module's [`open()`] reverses the
//! construction using the worker's long-lived [`StaticSecret`].
//!
//! ## Algorithm constants (mirror of server)
//!
//! - X25519 ECDH + HKDF-SHA256 + ChaCha20-Poly1305 AEAD.
//! - AAD binds `<alg>|v=<v>` so a future algorithm change rejects
//!   forged-as-old envelopes with a clean auth failure.
//! - Nonce is derived from the ECDH shared secret (not transmitted) — one
//!   ephemeral server key per call gives a unique shared secret + unique
//!   derived nonce; AEAD nonce reuse is structurally impossible.
//!
//! Phase 5a (server#107, v2.32.0) introduced these primitives on the server
//! side; Phase 5b (server#109) added the `/sealed` endpoint; this 5c round
//! closes Phase 5.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use hkdf::Hkdf;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

use anyhow::{anyhow, Context, Result};

/// Algorithm identifier carried on the wire + bound into the AEAD AAD.
///
/// MUST match `noetl/server`'s `crypto::sealed::SEAL_ALG`.  A change here
/// without a matching change on the server is a clean auth failure rather
/// than a silent compatibility break.
pub const SEAL_ALG: &str = "x25519-hkdf-sha256-chacha20-poly1305";

/// Wire format version.  Must match server's `SEAL_V`.
pub const SEAL_V: u8 = 1;

/// HKDF "info" prefix — same as server's `KDF_INFO`.
const KDF_INFO: &[u8] = b"noetl-sealed-v1";

/// Sealed envelope as it arrives on the wire from `GET
/// /api/credentials/{id}/sealed`.  Identical shape to server's struct.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealedEnvelope {
    pub alg: String,
    pub v: u8,
    pub eph_pub: String,
    pub ciphertext: String,
}

/// Open a [`SealedEnvelope`] addressed to `recipient_sk`.
///
/// Decodes the wire fields, runs X25519 ECDH against the server's ephemeral
/// public key, derives the AEAD key + nonce via HKDF-SHA256, and verifies +
/// decrypts the ChaCha20-Poly1305 ciphertext.  The associated data binds
/// `<alg>|v=<v>` so any tampering with `alg` or `v` is a clean auth failure.
///
/// The returned `Vec<u8>` is the plaintext credential bytes.  The caller is
/// responsible for `zeroize::Zeroize`ing them after use.
pub fn open(recipient_sk: &StaticSecret, env: &SealedEnvelope) -> Result<Vec<u8>> {
    if env.alg != SEAL_ALG {
        return Err(anyhow!(
            "sealed open: unsupported alg '{}' (expected '{SEAL_ALG}')",
            env.alg
        ));
    }
    if env.v != SEAL_V {
        return Err(anyhow!(
            "sealed open: unsupported version {} (expected {SEAL_V})",
            env.v
        ));
    }
    let eph_pub_bytes = B64
        .decode(&env.eph_pub)
        .context("sealed open: eph_pub base64")?;
    let eph_pub_array: [u8; 32] = eph_pub_bytes.as_slice().try_into().map_err(|_| {
        anyhow!(
            "sealed open: eph_pub must be 32 bytes, got {}",
            eph_pub_bytes.len()
        )
    })?;
    let eph_pk = PublicKey::from(eph_pub_array);
    let ciphertext = B64
        .decode(&env.ciphertext)
        .context("sealed open: ciphertext base64")?;

    let shared = recipient_sk.diffie_hellman(&eph_pk);
    let (key, nonce) = derive_key_nonce(shared.as_bytes())?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    cipher
        .decrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: &ciphertext,
                aad: associated_data().as_bytes(),
            },
        )
        .map_err(|e| anyhow!("sealed open: AEAD verify/decrypt: {e}"))
}

/// Domain-separated HKDF that produces a 32-byte AEAD key + 12-byte nonce
/// from the ECDH shared secret.  Bit-identical to server's `derive_key_nonce`.
fn derive_key_nonce(shared: &[u8; 32]) -> Result<([u8; 32], [u8; 12])> {
    let hkdf = Hkdf::<Sha256>::new(None, shared);
    let mut okm = [0u8; 32 + 12];
    hkdf.expand(KDF_INFO, &mut okm)
        .map_err(|e| anyhow!("sealed kdf: {e}"))?;
    let mut key = [0u8; 32];
    let mut nonce = [0u8; 12];
    key.copy_from_slice(&okm[..32]);
    nonce.copy_from_slice(&okm[32..]);
    Ok((key, nonce))
}

/// Associated data bound into the AEAD.  MUST match server's helper of the
/// same name — any divergence rejects every envelope as a clean auth
/// failure.
fn associated_data() -> String {
    format!("{SEAL_ALG}|v={SEAL_V}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chacha20poly1305::aead::{Aead, KeyInit, Payload};
    use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
    use x25519_dalek::EphemeralSecret;

    /// Local seal helper that mirrors the server's `seal()` exactly.  Lets
    /// the worker-side `open()` be unit-tested without spinning up a full
    /// server.  Drift between this and `noetl/server/src/crypto/sealed.rs`
    /// would show up as a tamper-rejection in real traffic + a unit-test
    /// failure here (the AAD + KDF constants are pinned in both places).
    fn seal(recipient_pk: &PublicKey, plaintext: &[u8]) -> SealedEnvelope {
        let eph_sk = EphemeralSecret::random_from_rng(rand_core::OsRng);
        let eph_pk = PublicKey::from(&eph_sk);
        let shared = eph_sk.diffie_hellman(recipient_pk);
        let (key, nonce) = derive_key_nonce(shared.as_bytes()).unwrap();
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
        let ciphertext = cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: associated_data().as_bytes(),
                },
            )
            .unwrap();
        SealedEnvelope {
            alg: SEAL_ALG.to_string(),
            v: SEAL_V,
            eph_pub: B64.encode(eph_pk.as_bytes()),
            ciphertext: B64.encode(&ciphertext),
        }
    }

    fn recipient() -> (StaticSecret, PublicKey) {
        let sk = StaticSecret::random_from_rng(rand_core::OsRng);
        let pk = PublicKey::from(&sk);
        (sk, pk)
    }

    #[test]
    fn round_trip_short_payload() {
        let (sk, pk) = recipient();
        let env = seal(&pk, b"hello-secret");
        let opened = open(&sk, &env).unwrap();
        assert_eq!(opened, b"hello-secret");
    }

    #[test]
    fn round_trip_realistic_credential_json() {
        // The exact shape the server's `/sealed` endpoint will return:
        // a full CredentialResponse including the bearer token + scope.
        let (sk, pk) = recipient();
        let plaintext = br#"{"id":"123","name":"duffel-token","type":"bearer","data":{"token":"sk-test-xyz","scope":"read"}}"#;
        let env = seal(&pk, plaintext);
        let opened = open(&sk, &env).unwrap();
        assert_eq!(&opened[..], plaintext);
    }

    #[test]
    fn open_rejects_tampered_ciphertext() {
        let (sk, pk) = recipient();
        let mut env = seal(&pk, b"important");
        let mut ct = B64.decode(&env.ciphertext).unwrap();
        ct[0] ^= 0x01;
        env.ciphertext = B64.encode(&ct);
        let err = open(&sk, &env).unwrap_err();
        assert!(format!("{err:?}").contains("AEAD verify/decrypt"));
    }

    #[test]
    fn open_rejects_unknown_alg() {
        let (sk, pk) = recipient();
        let mut env = seal(&pk, b"x");
        env.alg = "x25519-hkdf-sha256-aes-gcm-v2".to_string();
        let err = open(&sk, &env).unwrap_err();
        assert!(format!("{err:?}").contains("unsupported alg"));
    }

    #[test]
    fn open_rejects_wrong_version() {
        let (sk, pk) = recipient();
        let mut env = seal(&pk, b"x");
        env.v = SEAL_V + 7;
        let err = open(&sk, &env).unwrap_err();
        assert!(format!("{err:?}").contains("unsupported version"));
    }

    #[test]
    fn open_rejects_short_eph_pub() {
        let (sk, pk) = recipient();
        let mut env = seal(&pk, b"x");
        env.eph_pub = B64.encode([0u8; 16]); // wrong length
        let err = open(&sk, &env).unwrap_err();
        assert!(format!("{err:?}").contains("eph_pub must be 32 bytes"));
    }

    #[test]
    fn open_rejects_wrong_recipient() {
        let (_, pk_alice) = recipient();
        let (sk_bob, _) = recipient();
        let env = seal(&pk_alice, b"for-alice");
        let err = open(&sk_bob, &env).unwrap_err();
        assert!(format!("{err:?}").contains("AEAD verify/decrypt"));
    }

    /// Constants drift guard — if a future contributor changes `SEAL_ALG`,
    /// `SEAL_V`, or `KDF_INFO` on either side without matching the other,
    /// real traffic rejects every envelope and `cargo test` here flags
    /// which constant moved.
    #[test]
    fn algorithm_constants_match_server_contract() {
        assert_eq!(SEAL_ALG, "x25519-hkdf-sha256-chacha20-poly1305");
        assert_eq!(SEAL_V, 1);
        assert_eq!(KDF_INFO, b"noetl-sealed-v1");
        assert_eq!(
            associated_data(),
            "x25519-hkdf-sha256-chacha20-poly1305|v=1"
        );
    }
}
