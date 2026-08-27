use std::sync::Arc;

use ed25519_dalek::VerifyingKey;

use crate::{protocol::ClientMethod, server::Server};

pub async fn get_users(
    server: &Arc<Server>,
    _verifying_key: VerifyingKey,
    socket: &Arc<crate::ws::EnclaveWebSocket>,
    pubkeys: Vec<String>,
) -> anyhow::Result<()> {
    let users = server.user_store.get_users(&pubkeys).await?;

    socket.send(&ClientMethod::Users { users }).await?;

    Ok(())
}
