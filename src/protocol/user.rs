use std::sync::Arc;

use axum::extract::ws::WebSocket;
use ed25519_dalek::VerifyingKey;
use tokio::sync::Mutex;

use crate::{
    protocol::{ClientMethod, send_socket},
    server::Server,
};

pub async fn get_users(
    server: &Arc<Server>,
    _verifying_key: VerifyingKey,
    socket: &Arc<Mutex<WebSocket>>,
    pubkeys: Vec<String>,
) -> anyhow::Result<()> {
    let users = server.user_store.get_users(&pubkeys).await?;

    send_socket(&mut *socket.lock().await, &ClientMethod::Users { users }).await?;

    Ok(())
}
