use std::{
    collections::HashMap,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, atomic::AtomicU16},
};

use axum::{
    extract::{WebSocketUpgrade, ws::WebSocket},
    response::Response,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use tokio::{
    net::UdpSocket,
    sync::{Mutex, OnceCell},
    task::JoinSet,
    time::Instant,
};

use crate::{
    crypto::SessionCipher,
    data::{config::Config, messages::MessageStore, users::UserMetaStore},
    protocol::{ClientMethod, read_loop},
    types::ClientMeta,
};
use x25519_dalek::{PublicKey as X25519Public, StaticSecret as X25519Secret};

pub struct VoiceConnection {
    pub addr: SocketAddr,
    pub channel_id: String,
    pub last_speaking_sent: Instant,
}

pub struct UserConnections {
    pub meta: ClientMeta,
    pub counter: AtomicU16,
    pub public_key: VerifyingKey,
    pub connections: Mutex<HashMap<u16, Arc<crate::ws::EnclaveWebSocket>>>,
    pub cihper: Arc<Mutex<SessionCipher>>,
    pub voice: Mutex<Option<VoiceConnection>>,
}

pub struct Server {
    pub key: SigningKey,
    pub x_keypair: (X25519Public, X25519Secret),
    pub config: Config,
    pub clients: Mutex<HashMap<VerifyingKey, Arc<UserConnections>>>,
    pub voice_pins: Mutex<HashMap<u64, (VerifyingKey, String)>>,
    pub message_store: MessageStore,
    pub user_store: UserMetaStore,
    pub voice_socket: OnceCell<UdpSocket>,
}

impl Server {
    pub async fn new() -> anyhow::Result<Arc<Self>> {
        let key = crate::crypto::get().await?;
        Ok(Arc::new(Self {
            x_keypair: (
                crate::crypto::ed25519_verifying_key_to_x25519(&key.verifying_key())
                    .ok_or(anyhow::anyhow!("Failed to convert ed pubkey to x"))?,
                crate::crypto::ed25519_signing_key_to_x25519(&key),
            ),
            key,
            config: Config::get().await?,
            clients: Mutex::new(HashMap::new()),
            voice_pins: Mutex::new(HashMap::new()),
            message_store: MessageStore::new(PathBuf::from("messages"))?,
            user_store: UserMetaStore::new(PathBuf::from("users.db"))?,
            voice_socket: OnceCell::new(),
        }))
    }
}

impl Server {
    pub async fn ws_handler(self: &Arc<Self>, ws: WebSocketUpgrade) -> Response {
        let s = self.clone();

        ws.on_upgrade(move |socket: WebSocket| async move {
            let mut client = match crate::crypto::crypto_handshake(&s, socket).await {
                Ok(client) => client,
                Err(err) => {
                    eprintln!("Failed to initialize crypto: {err}");
                    return;
                }
            };

            match UserConnections::initialize(&s, &client).await {
                Ok((public_key, meta)) => {
                    if let Err(e) = s
                        .user_store
                        .upsert_user(&crate::crypto::to_string(&public_key), &meta)
                        .await
                    {
                        eprintln!("Failed to upsert client: {e}");
                    }

                    let mut clients_meta = s.clients.lock().await;

                    let clients = clients_meta
                        .entry(public_key)
                        .or_insert_with(|| {
                            Arc::new(UserConnections {
                                meta,
                                public_key: public_key,
                                counter: AtomicU16::new(0),
                                connections: Mutex::new(HashMap::new()),
                                voice: Mutex::new(None),
                                cihper: client.cipher.clone(),
                            })
                        })
                        .clone();

                    client.cipher = clients.cihper.clone();

                    let client = Arc::new(client);

                    let conid = clients
                        .counter
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                    clients
                        .connections
                        .lock()
                        .await
                        .insert(conid, client.clone());

                    drop(clients_meta);

                    if let Err(e) = read_loop(&s, public_key, &client).await {
                        eprintln!("Failed to handle client: {e}");
                    }

                    let mut clients_meta = s.clients.lock().await;

                    let mut connections = clients.connections.lock().await;

                    connections.remove(&conid);

                    if connections.is_empty() {
                        clients_meta.remove(&public_key);

                        if let Some(voice) = clients.voice.lock().await.take() {
                            s.voice_pins
                                .lock()
                                .await
                                .retain(|_, v| v.0 != public_key);

                            s.broadcast(&crate::protocol::ClientMethod::UserLeftVoice {
                                channel_id: voice.channel_id,
                                pubkey: crate::crypto::to_string(&public_key),
                            })
                            .await
                            .ok();
                        }
                    }
                }

                Err(e) => {
                    eprintln!("Failed to initialize client: {e}")
                }
            }
        })
    }

    pub async fn broadcast(self: &Arc<Self>, message: &ClientMethod) -> anyhow::Result<()> {
        let mut set = JoinSet::new();

        for (_, client) in self.clients.lock().await.iter() {
            let msg = message.clone();
            let client = client.clone();

            set.spawn(async move { client.send(&msg).await });
        }

        // Await all spawned tasks to finish
        while let Some(res) = set.join_next().await {
            // handle task panic or errors if necessary
            if let Ok(Err(e)) = res {
                eprintln!("Failed to send to a client: {:?}", e);
            }
        }

        Ok(())
    }
}

impl UserConnections {
    pub async fn send(&self, message: &ClientMethod) -> anyhow::Result<()> {
        for (_, conn) in self.connections.lock().await.iter() {
            conn.send(message).await?;
        }

        Ok(())
    }

    pub async fn send_to(&self, id: u16, message: &ClientMethod) -> anyhow::Result<bool> {
        if let Some(conn) = self.connections.lock().await.get(&id) {
            conn.send(message).await?;

            Ok(true)
        } else {
            Ok(false)
        }
    }
}
