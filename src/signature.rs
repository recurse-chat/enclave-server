use std::path::PathBuf;

use ed25519_dalek::SigningKey;
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
