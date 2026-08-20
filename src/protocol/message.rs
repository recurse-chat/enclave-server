use crate::data::messages::{MessageData, StoredMessage};
use crate::protocol::{ClientMethod, send_socket};
use crate::server::Server;
use axum::extract::ws::WebSocket;
use ed25519_dalek::{Verifier, VerifyingKey};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

pub async fn send_message(
    server: &Arc<Server>,
    verifying_key: VerifyingKey,
    _socket: &Arc<Mutex<WebSocket>>,
    message: MessageData,
    channel_id: String,
) -> anyhow::Result<()> {
    let server_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

    if server_timestamp.saturating_sub(message.timestamp) > 2000 {
        anyhow::bail!(
            "Message timestamp out of range ({server_timestamp} - {}) < 2000",
            message.timestamp
        );
    }

    let server_pubkey_string = crate::signature::to_string(&server.key.verifying_key());
    let signed_string = format!(
        "{}@{}@{}",
        message.timestamp, server_pubkey_string, message.content
    );

    let signature = crate::signature::from_string_sig(&message.signature)
        .map_err(|_| anyhow::anyhow!("Invalid signature encoding"))?;

    verifying_key
        .verify(signed_string.as_bytes(), &signature)
        .map_err(|_| anyhow::anyhow!("Signature verification failed"))?;

    let stored = StoredMessage {
        id: uuid::Uuid::new_v4().to_string(),
        author: crate::signature::to_string(&verifying_key),
        data: message,
    };

    server.message_store.insert_message(&channel_id, &stored)?;

    Ok(())
}

pub async fn get_messages(
    server: &Arc<Server>,
    _verifying_key: VerifyingKey,
    socket: &Arc<Mutex<WebSocket>>,
    channel_id: String,
    chunk: u32,
) -> anyhow::Result<()> {
    const CHUNK_SIZE: u32 = 16;

    let messages = server
        .message_store
        .get_recent_messages(&channel_id, CHUNK_SIZE, chunk)?;

    send_socket(
        &mut *socket.lock().await,
        &ClientMethod::Messages {
            messages: HashMap::from([(channel_id, messages)]),
        },
    )
    .await?;

    Ok(())
}
