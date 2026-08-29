use ed25519_dalek::SigningKey;
use x25519_dalek::{PublicKey as X25519Public, StaticSecret as X25519Secret};

pub struct ServerIdentity {
    pub key: SigningKey,
    pub x25519: X25519KeyPair,
}

pub struct X25519KeyPair {
    pub public: X25519Public,
    pub secret: X25519Secret,
}

impl ServerIdentity {
    pub async fn load() -> anyhow::Result<Self> {
        let key = crate::crypto::get().await?;

        let x25519 = X25519KeyPair {
            public: crate::crypto::ed25519_verifying_key_to_x25519(&key.verifying_key())
                .ok_or(anyhow::anyhow!("Failed to convert ed pubkey to x"))?,
            secret: crate::crypto::ed25519_signing_key_to_x25519(&key),
        };

        Ok(Self { key, x25519 })
    }
}