use std::{borrow::Cow, sync::Arc};

use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

pub mod initialize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMeta {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerMeta {
    pub name: String,
    pub description: String,
}

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

    Meta(ClientMeta),

    Error {
        error: String,
    },
}

pub async fn read_loop(socket: &Arc<Mutex<WebSocket>>) -> anyhow::Result<()> {
    while let Some(message) = read_socket(&mut *socket.lock().await).await? {
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
        }
    }

    Ok(())
}

pub async fn read_socket(socket: &mut WebSocket) -> anyhow::Result<Option<ServerMethod>> {
    match socket.recv().await.transpose()? {
        Some(Message::Text(text)) => {
            if let Ok(msg) = serde_json::from_str(&text.to_string()) {
                Ok(Some(msg))
            } else {
                send_socket(
                    socket,
                    &ClientMethod::Error {
                        error: Cow::Borrowed("Unable to parse message: {text}"),
                    },
                )
                .await?;

                Ok(None)
            }
        }

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
