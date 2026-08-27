use std::borrow::Cow;

use axum::extract::ws::{Message, Utf8Bytes, WebSocket};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::sync::Mutex;

use crate::protocol::{ClientMethod, ServerMethod};

pub struct EnclaveWebSocket {
    tx: Mutex<SplitSink<WebSocket, Message>>,
    rx: Mutex<SplitStream<WebSocket>>,
}

impl EnclaveWebSocket {
    pub fn new(ws: WebSocket) -> Self {
        let (tx, rx) = ws.split();

        Self {
            tx: Mutex::new(tx),
            rx: Mutex::new(rx),
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

            Some(Message::Ping(v)) => {
                self.tx.lock().await.send(Message::Pong(v)).await?;

                Ok(None)
            }

            Some(_) => Ok(None),

            None => Ok(None),
        }
    }

    pub async fn send(&self, message: &ClientMethod) -> anyhow::Result<()> {
        self.tx
            .lock()
            .await
            .send(Message::Text(Utf8Bytes::from(serde_json::to_string(
                message,
            )?)))
            .await?;

        Ok(())
    }
}
