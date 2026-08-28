use std::borrow::Cow;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::sync::Mutex;

use crate::{
    crypto::SessionCipher,
    protocol::{ClientMethod, ServerMethod},
};

pub struct EnclaveWebSocket {
    tx: Mutex<SplitSink<WebSocket, Message>>,
    rx: Mutex<SplitStream<WebSocket>>,
    cihper: Mutex<SessionCipher>,
}

impl EnclaveWebSocket {
    pub fn new(ws: WebSocket, cipher: SessionCipher) -> Self {
        let (tx, rx) = ws.split();

        Self {
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
            cihper: Mutex::new(cipher),
        }
    }

    pub async fn read(&self) -> anyhow::Result<Option<ServerMethod>> {
        match self.rx.lock().await.next().await.transpose()? {
            Some(Message::Text(text)) => match serde_json::from_str(&text.to_string()) {
                Ok(msg) => Ok(Some(msg)),

                Err(e) => {
                    self.send(&ClientMethod::Error {
                        error: Cow::Owned(format!("Unable to parse message: {e}")),
                    })
                    .await?;

                    Ok(None)
                }
            },

            Some(Message::Binary(encrypted)) => {
                let text = String::from_utf8(self.cihper.lock().await.decrypt(&encrypted)?)?;

                match serde_json::from_str(&text.to_string()) {
                    Ok(msg) => Ok(Some(msg)),

                    Err(e) => {
                        self.send(&ClientMethod::Error {
                            error: Cow::Owned(format!("Unable to parse message: {e}")),
                        })
                        .await?;

                        Ok(None)
                    }
                }
            }

            Some(Message::Ping(v)) => {
                self.tx.lock().await.send(Message::Pong(v)).await?;

                Ok(None)
            }

            Some(_) => Ok(None),

            None => Ok(None),
        }
    }

    pub async fn send(&self, message: &ClientMethod) -> anyhow::Result<()> {
        let text = serde_json::to_string(message)?;

        let encrypted = self.cihper.lock().await.encrypt(text.as_bytes())?;

        self.tx
            .lock()
            .await
            .send(Message::Binary(encrypted.into()))
            .await?;

        Ok(())
    }
}
