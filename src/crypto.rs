use std::{path::PathBuf, sync::Arc};

use axum::extract::ws::WebSocket;
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, KeyInit},
};
use curve25519_dalek::edwards::CompressedEdwardsY;
use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use rand::rngs::OsRng;
use sha2::{Digest, Sha512};
use x25519_dalek::{PublicKey as X25519Public, SharedSecret, StaticSecret as X25519Secret};

use crate::{server::Server, ws::EnclaveWebSocket};

pub async fn get() -> anyhow::Result<SigningKey> {
    let private_key_path = PathBuf::from("private.key");

    if !private_key_path.exists() {
        let key = SigningKey::generate(&mut OsRng);
        tokio::fs::write(private_key_path, &key.to_bytes()).await?;
        Ok(key)
    } else {
        Ok(SigningKey::from_bytes(
            &tokio::fs::read(private_key_path)
                .await?
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid private key"))?,
        ))
    }
}

pub fn to_string(key: &VerifyingKey) -> String {
    bs58::encode(key.to_bytes()).into_string()
}

pub fn to_string_sig(signature: &Signature) -> String {
    bs58::encode(signature.to_bytes()).into_string()
}

pub fn from_string(key: &str) -> anyhow::Result<VerifyingKey> {
    Ok(VerifyingKey::from_bytes(
        &bs58::decode(key)
            .into_vec()?
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid public key"))?,
    )?)
}

pub fn from_string_sig(signature: &str) -> anyhow::Result<Signature> {
    Ok(Signature::from_bytes(
        &bs58::decode(signature)
            .into_vec()?
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid signature"))?,
    ))
}

pub fn ed25519_signing_key_to_x25519(signing_key: &SigningKey) -> X25519Secret {
    let hash = Sha512::digest(signing_key.as_bytes());
    let mut scalar_bytes = [0u8; 32];
    scalar_bytes.copy_from_slice(&hash[..32]);
    X25519Secret::from(scalar_bytes) // clamping happens internally
}

pub fn ed25519_verifying_key_to_x25519(verifying_key: &VerifyingKey) -> Option<X25519Public> {
    let compressed = CompressedEdwardsY(verifying_key.to_bytes());
    let edwards_point = compressed.decompress()?;
    Some(X25519Public::from(edwards_point.to_montgomery().to_bytes()))
}

pub struct SessionCipher {
    cipher: ChaCha20Poly1305,
    send_counter: u64,
    recv_counter: u64,
}

impl SessionCipher {
    pub fn new(shared_secret: &SharedSecret) -> anyhow::Result<Self> {
        // SharedSecret's raw bytes ARE suitable for direct use as a ChaCha20Poly1305 key
        // (both are 32 bytes), though in a hardened design you'd typically run this
        // through a KDF (e.g. HKDF) rather than using the raw ECDH output directly.
        let key = Key::try_from(shared_secret.as_bytes().as_slice())?;

        Ok(Self {
            cipher: ChaCha20Poly1305::new(&key),
            send_counter: 0,
            recv_counter: 0,
        })
    }

    pub fn next_send_nonce(&mut self) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&self.send_counter.to_be_bytes());
        // top bit distinguishes "send" direction from "recv" direction,
        // so client-send and server-send counters never collide even if
        // both happened to reach the same numeric value
        nonce[11] |= 0b1000_0000;
        self.send_counter += 1;
        nonce
    }

    pub fn next_recv_nonce(&mut self) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&self.recv_counter.to_be_bytes());
        self.recv_counter += 1;
        nonce
    }

    pub fn encrypt(&mut self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
        let nonce_bytes = self.next_send_nonce();
        let nonce = Nonce::try_from(nonce_bytes)?;

        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|_| anyhow::anyhow!("encryption failed"))?;

        // prepend the nonce so the other side can reconstruct it on decrypt
        let mut out = nonce_bytes.to_vec();
        out.extend(ciphertext);
        Ok(out)
    }

    pub fn decrypt(&mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        if data.len() < 12 {
            anyhow::bail!("message too short to contain a nonce");
        }
        let (nonce_bytes, ciphertext) = data.split_at(12);
        let nonce = Nonce::try_from(nonce_bytes)?;

        self.cipher
            .decrypt(&nonce, ciphertext)
            .map_err(|_| anyhow::anyhow!("decryption failed"))
    }
}

pub async fn crypto_handshake(
    server: &Arc<Server>,
    mut socket: WebSocket,
) -> anyhow::Result<Arc<EnclaveWebSocket>> {
    socket
        .send(axum::extract::ws::Message::Binary(
            server.x_keypair.0.to_bytes().to_vec().into(),
        ))
        .await?;

    let axum::extract::ws::Message::Binary(raw_pubkey) = socket
        .recv()
        .await
        .transpose()?
        .ok_or(anyhow::anyhow!("Failed to get client x key"))?
    else {
        return Err(anyhow::anyhow!(""));
    };

    let client_pubkey = X25519Public::from(*raw_pubkey.as_array().ok_or(anyhow::anyhow!(
        "Failed to get proper length of client x key"
    ))?);

    let shared_secret = server.x_keypair.1.diffie_hellman(&client_pubkey);

    let cipher = SessionCipher::new(&shared_secret)?;

    Ok(Arc::new(EnclaveWebSocket::new(socket, cipher)))
}
