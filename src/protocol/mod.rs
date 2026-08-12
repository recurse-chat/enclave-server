use std::borrow::Cow;

use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use serde::{Deserialize, Serialize};

pub mod initialize;

pub struct Client {
    pub socket: WebSocket,
    pub meta: ClientMeta,
    pub public_key: String,
}

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
        timestamp: u64,
        signature: String,
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
        timestamp: u64,
        signature: String,
    },

    Error {
        error: String,
    },
}

impl Client {
    pub async fn read_loop(&mut self) -> anyhow::Result<()> {
        while let Some(message) = self.read().await? {
            match message {
                ServerMethod::Initialize { .. } => {
                    self.send(ClientMethod::Error {
                        error: Cow::Borrowed("Already initialized"),
                    })
                    .await?;
                }

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
                    Client::send_socket(
                        socket,
                        ClientMethod::Error {
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

    pub async fn send_socket(socket: &mut WebSocket, message: ClientMethod) -> anyhow::Result<()> {
        socket
            .send(Message::Text(Utf8Bytes::from(serde_json::to_string(
                &message,
            )?)))
            .await?;

        Ok(())
    }

    pub async fn read(&mut self) -> anyhow::Result<Option<ServerMethod>> {
        Self::read_socket(&mut self.socket).await
    }

    pub async fn send(&mut self, message: ClientMethod) -> anyhow::Result<()> {
        Self::send_socket(&mut self.socket, message).await
    }
}
