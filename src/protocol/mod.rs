use std::{borrow::Cow, collections::HashMap, sync::Arc};

use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use ed25519_dalek::VerifyingKey;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{data::messages::StoredMessage, server::Server, types::ClientMeta};

pub mod initialize;
pub mod message;
pub mod user;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum ClientMethod {
    Initialized {
        public_key: String,
        signature: String,

        timestamp: u64,
        hostname: String,
    },

    Error {
        error: Cow<'static, str>,
    },

    Messages {
        messages: HashMap<String, Vec<StoredMessage>>,
    },

    Users {
        users: HashMap<String, ClientMeta>,
    },

    MessageEdited {
        channel_id: String,
        message: StoredMessage,
    },

    MessageDeleted {
        channel_id: String,
        message_id: String,
    },

    JoinVoice {
        channel_id: String,
        pin: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum ServerMethod {
    Initialize {
        public_key: String,
        signature: String,

        timestamp: u64,
        hostname: String,
    },

    Error {
        error: String,
    },

    SendMessage {
        channel_id: String,
        data: crate::data::messages::MessageData,
    },

    GetMessages {
        channel_id: String,
        chunk: u32,
    },

    Meta(ClientMeta),

    GetUsers {
        pubkeys: Vec<String>,
    },

    EditMessage {
        message_id: String,
        channel_id: String,
        content: String,
        signature: String,
    },

    DeleteMessage {
        message_id: String,
        channel_id: String,
    },

    JoinVoice {
        channel_id: String,
    },
}

pub async fn read_loop(
    server: &Arc<Server>,
    verifying_key: VerifyingKey,
    socket: &Arc<Mutex<WebSocket>>,
) -> anyhow::Result<()> {
    let mut socket_lock = socket.lock().await;

    while let Some(message) = read_socket(&mut *socket_lock).await? {
        drop(socket_lock);

        match message {
            ServerMethod::Initialize { .. } => {
                send_socket(
                    &mut *socket.lock().await,
                    &ClientMethod::Error {
                        error: Cow::Borrowed("Already initialized"),
                    },
                )
                .await?;
            }

            #[allow(unused_variables)]
            ServerMethod::Meta(meta) => {}

            ServerMethod::Error { error } => {
                eprintln!("Client error: {error}");
            }

            ServerMethod::SendMessage { channel_id, data } => {
                message::send_message(server, verifying_key, socket, data, channel_id).await?;
            }

            ServerMethod::GetMessages { channel_id, chunk } => {
                message::get_messages(server, verifying_key, socket, channel_id, chunk).await?;
            }

            ServerMethod::DeleteMessage {
                message_id,
                channel_id,
            } => {
                message::delete_message(server, verifying_key, message_id, channel_id).await?;
            }

            ServerMethod::EditMessage {
                message_id,
                channel_id,
                content,
                signature,
            } => {
                message::edit_message(
                    server,
                    verifying_key,
                    message_id,
                    channel_id,
                    content,
                    signature,
                )
                .await?;
            }

            ServerMethod::GetUsers { pubkeys } => {
                user::get_users(server, verifying_key, socket, pubkeys).await?;
            }

            ServerMethod::JoinVoice { channel_id } => {
                let pin = rand::random::<u64>() % (1 << 53);

                server
                    .voice_pins
                    .lock()
                    .await
                    .insert(pin, (verifying_key, channel_id.clone()));

                send_socket(
                    &mut *socket.lock().await,
                    &ClientMethod::JoinVoice { channel_id, pin },
                )
                .await?;
            }
        }

        socket_lock = socket.lock().await;
    }

    Ok(())
}

pub async fn read_socket(socket: &mut WebSocket) -> anyhow::Result<Option<ServerMethod>> {
    match socket.recv().await.transpose()? {
        Some(Message::Text(text)) => match serde_json::from_str(&text.to_string()) {
            Ok(msg) => Ok(Some(msg)),

            Err(e) => {
                send_socket(
                    socket,
                    &ClientMethod::Error {
                        error: Cow::Owned(format!("Unable to parse message: {e}")),
                    },
                )
                .await?;

                Ok(None)
            }
        },

        Some(Message::Ping(v)) => {
            socket.send(Message::Pong(v)).await?;

            Ok(None)
        }

        Some(_) => Ok(None),

        None => Ok(None),
    }
}

pub async fn send_socket(socket: &mut WebSocket, message: &ClientMethod) -> anyhow::Result<()> {
    socket
        .send(Message::Text(Utf8Bytes::from(serde_json::to_string(
            message,
        )?)))
        .await?;

    Ok(())
}
