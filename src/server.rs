use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU16},
};

use axum::{
    extract::{WebSocketUpgrade, ws::WebSocket},
    response::Response,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use tokio::sync::Mutex;

use crate::{
    config::Config,
    protocol::{Client, ClientMeta},
};

pub struct OnlineClientMeta {
    pub meta: ClientMeta,
    pub counter: AtomicU16,
    pub connections: HashMap<u16, Client>,
}

pub struct Server {
    pub key: SigningKey,
    pub config: Config,
    pub clients: Mutex<HashMap<VerifyingKey, OnlineClientMeta>>,
}

impl Server {
    pub async fn new() -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            key: crate::signature::get().await?,
            config: Config::get().await?,
            clients: Mutex::new(HashMap::new()),
        }))
    }
}

impl Server {
    pub async fn ws_handler(self: &Arc<Self>, ws: WebSocketUpgrade) -> Response {
        let s = self.clone();

        ws.on_upgrade(move |socket: WebSocket| async move {
            match Client::initialize(&s, socket).await {
                Ok((mut client, meta)) => {
                    let mut clients_meta = s.clients.lock().await;

                    let client_meta =
                        clients_meta
                            .entry(client.public_key)
                            .or_insert_with(|| OnlineClientMeta {
                                meta,
                                counter: AtomicU16::new(0),
                                connections: HashMap::new(),
                            });

                    client_meta.connections.insert(
                        client_meta
                            .counter
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                        client.clone(),
                    );

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
