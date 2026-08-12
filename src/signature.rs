use std::path::PathBuf;

use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
use rand::rngs::OsRng;

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
            .map_err(|_| anyhow::anyhow!("Invalid public key"))?,
    ))
}
