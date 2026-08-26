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

    let server_pubkey_string = crate::crypto::to_string(&server.key.verifying_key());
    let signed_string = format!(
        "{}@{}@{}",
        message.timestamp, server_pubkey_string, message.content
    );

    let signature = crate::crypto::from_string_sig(&message.signature)
        .map_err(|_| anyhow::anyhow!("Invalid signature encoding"))?;

    verifying_key
        .verify(signed_string.as_bytes(), &signature)
        .map_err(|_| anyhow::anyhow!("Signature verification failed"))?;

    let stored = StoredMessage {
        id: uuid::Uuid::new_v4().to_string(),
        author: crate::crypto::to_string(&verifying_key),
        is_edited: false,
        data: message,
    };

    server.message_store.insert_message(&channel_id, &stored)?;

    server
        .broadcast(&ClientMethod::Messages {
            messages: HashMap::from([(channel_id, vec![stored])]),
        })
        .await?;

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

pub async fn edit_message(
    server: &Arc<Server>,
    verifying_key: VerifyingKey,
    message_id: String,
    channel_id: String,
    new_content: String,
    new_signature: String,
) -> anyhow::Result<()> {
    let existing = server
        .message_store
        .get_message(&channel_id, &message_id)?
        .ok_or_else(|| anyhow::anyhow!("Message not found"))?;

    let author_pubkey = crate::crypto::to_string(&verifying_key);
    if existing.author != author_pubkey {
        anyhow::bail!("Not authorized to edit this message");
    }

    let server_pubkey_string = crate::crypto::to_string(&server.key.verifying_key());
    let signed_string = format!(
        "{}@{}@{}",
        existing.data.timestamp, server_pubkey_string, new_content
    );

    let signature = crate::crypto::from_string_sig(&new_signature)
        .map_err(|_| anyhow::anyhow!("Invalid signature encoding"))?;

    verifying_key
        .verify(signed_string.as_bytes(), &signature)
        .map_err(|_| anyhow::anyhow!("Signature verification failed"))?;

    server
        .message_store
        .update_message(&channel_id, &message_id, &new_content, &new_signature)?;

    let updated = StoredMessage {
        id: message_id,
        author: author_pubkey,
        is_edited: true,
        data: MessageData {
            content: new_content,
            timestamp: existing.data.timestamp,
            signature: new_signature,
        },
    };

    server
        .broadcast(&ClientMethod::MessageEdited {
            channel_id: channel_id.clone(),
            message: updated,
        })
        .await?;

    Ok(())
}

pub async fn delete_message(
    server: &Arc<Server>,
    verifying_key: VerifyingKey,
    message_id: String,
    channel_id: String,
) -> anyhow::Result<()> {
    let existing = server
        .message_store
        .get_message(&channel_id, &message_id)?
        .ok_or_else(|| anyhow::anyhow!("Message not found"))?;

    let author_pubkey = crate::crypto::to_string(&verifying_key);
    if existing.author != author_pubkey {
        anyhow::bail!("Not authorized to delete this message");
    }

    server
        .message_store
        .delete_message(&channel_id, &message_id)?;

    server
        .broadcast(&ClientMethod::MessageDeleted {
            channel_id: channel_id.clone(),
            message_id,
        })
        .await?;

    Ok(())
}
