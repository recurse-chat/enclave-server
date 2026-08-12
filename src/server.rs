use std::sync::Arc;

use axum::{
    extract::{WebSocketUpgrade, ws::WebSocket},
    response::Response,
};
use ed25519_dalek::SigningKey;

use crate::protocol::Client;

pub struct Server {
    pub key: SigningKey,
}

impl Server {
    pub async fn new() -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            key: crate::signature::get().await?,
        }))
    }
}

impl Server {
    pub async fn ws_handler(self: &Arc<Self>, ws: WebSocketUpgrade) -> Response {
        ws.on_upgrade(|socket: WebSocket| async {
            match Client::initialize(socket).await {
                Ok(mut client) => {
                    if let Err(e) = client.read_loop().await {
                        eprintln!("Failed to handle client: {e}");
                    } else {
                        println!("Client connection closed")
                    }
                }

                Err(e) => {
                    eprintln!("Failed to initialize client: {e}")
                }
            }
        })
    }
}
