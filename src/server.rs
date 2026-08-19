use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, atomic::AtomicU16},
};

use axum::{
    extract::{WebSocketUpgrade, ws::WebSocket},
    response::Response,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use tokio::sync::Mutex;

use crate::{
    data::{config::Config, messages::MessageStore},
    protocol::{ClientMethod, read_loop, send_socket},
    types::ClientMeta,
};

pub struct UserConnections {
    pub meta: ClientMeta,
    pub counter: AtomicU16,
    pub public_key: VerifyingKey,
    pub connections: HashMap<u16, Arc<Mutex<WebSocket>>>,
}

pub struct Server {
    pub key: SigningKey,
    pub config: Config,
    pub clients: Mutex<HashMap<VerifyingKey, UserConnections>>,
    pub message_store: MessageStore,
}

impl Server {
    pub async fn new() -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            key: crate::signature::get().await?,
            config: Config::get().await?,
            clients: Mutex::new(HashMap::new()),
            message_store: MessageStore::new(PathBuf::from("item"))?,
        }))
    }
}

impl Server {
    pub async fn ws_handler(self: &Arc<Self>, ws: WebSocketUpgrade) -> Response {
        let s = self.clone();

        ws.on_upgrade(move |socket: WebSocket| async move {
            match UserConnections::initialize(&s, socket).await {
                Ok((client, public_key, meta)) => {
                    let client = Arc::new(Mutex::new(client));

                    let mut clients_meta = s.clients.lock().await;

                    let clients =
                        clients_meta
                            .entry(public_key)
                            .or_insert_with(|| UserConnections {
                                meta,
                                public_key: public_key,
                                counter: AtomicU16::new(0),
                                connections: HashMap::new(),
                            });

                    let conid = clients
                        .counter
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    clients.connections.insert(conid, client.clone());

                    if let Err(e) = read_loop(&client).await {
                        eprintln!("Failed to handle client: {e}");
                    } else {
                        println!("Client connection closed")
                    }

                    clients.connections.remove(&conid);

                    if clients.connections.len() == 0 {
                        clients_meta.remove(&public_key);
                    }
                }

                Err(e) => {
                    eprintln!("Failed to initialize client: {e}")
                }
            }
        })
    }
}

impl UserConnections {
    pub async fn send(&self, message: &ClientMethod) -> anyhow::Result<()> {
        for (_, conn) in &self.connections {
            send_socket(&mut *conn.lock().await, message).await?;
        }

        Ok(())
    }

    pub async fn send_to(&self, id: u16, message: &ClientMethod) -> anyhow::Result<bool> {
        if let Some(conn) = self.connections.get(&id) {
            send_socket(&mut *conn.lock().await, message).await?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}
