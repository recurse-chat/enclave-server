use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use serde::{Deserialize, Serialize};

pub mod initialize;

pub struct Client {
    pub socket: WebSocket,
    pub meta: ClientMeta,
    pub pub_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMeta {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage<'a> {
    Error(&'a str),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    Initialize(),
}

impl Client {
    pub async fn read_socket(socket: &mut WebSocket) -> anyhow::Result<Option<ServerMessage>> {
        match socket.recv().await.transpose()? {
            Some(Message::Text(text)) => {
                if let Ok(msg) = serde_json::from_str(&text.to_string()) {
                    Ok(Some(msg))
                } else {
                    Client::send_socket(
                        socket,
                        ClientMessage::Error("Unable to parse message: {text}"),
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

    pub async fn send_socket<'a>(
        socket: &mut WebSocket,
        message: ClientMessage<'a>,
    ) -> anyhow::Result<()> {
        socket
            .send(Message::Text(Utf8Bytes::from(serde_json::to_string(
                &message,
            )?)))
            .await?;

        Ok(())
    }

    pub async fn read(&mut self) -> anyhow::Result<Option<ServerMessage>> {
        Self::read_socket(&mut self.socket).await
    }

    pub async fn send<'a>(&mut self, message: ClientMessage<'a>) -> anyhow::Result<()> {
        Self::send_socket(&mut self.socket, message).await
    }
}
