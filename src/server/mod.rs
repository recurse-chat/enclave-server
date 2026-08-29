pub mod identity;
pub mod session;
pub mod store;
pub mod vc_server;

use std::sync::Arc;

use axum::{
    extract::{WebSocketUpgrade, ws::WebSocket},
    response::Response,
};

use crate::{
    data::config::Config,
    protocol::{read_loop, ClientMethod},
};

pub use identity::{ServerIdentity, X25519KeyPair};
pub use session::{SessionRegistry, UserConnections};
pub use store::DataStore;
pub use vc_server::{VoiceConnection, VoicePin, VoiceServer};

pub struct Server {
    pub identity: ServerIdentity,
    pub config: Config,
    pub sessions: SessionRegistry,
    pub voice: Arc<VoiceServer>,
    pub store: DataStore,
}

impl Server {
    pub async fn new() -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            identity: ServerIdentity::load().await?,
            config: Config::get().await?,
            sessions: SessionRegistry::new(),
            voice: Arc::new(VoiceServer::new()),
            store: DataStore::new()?,
        }))
    }

    pub async fn ws_handler(self: &Arc<Self>, ws: WebSocketUpgrade) -> Response {
        let s = self.clone();

        ws.on_upgrade(move |socket: WebSocket| async move {
            let client = match crate::crypto::crypto_handshake(&s, socket).await {
                Ok(client) => client,
                Err(err) => {
                    log::error!("Failed to initialize crypto: {err}");
                    return;
                }
            };

            match crate::protocol::initialize::initialize(&s, &client).await {
                Ok((public_key, meta)) => {
                    if let Err(e) = s
                        .store
                        .users
                        .upsert_user(&crate::crypto::to_string(&public_key), &meta)
                        .await
                    {
                        log::error!("Failed to upsert client: {e}");
                    }

                    let pubkey_string = crate::crypto::to_string(&public_key);

                    log::info!("Client connected: {pubkey_string}");

                    let (client, conid) = s.sessions.register(public_key, meta, client).await;

                    if let Err(e) = read_loop(&s, public_key, &client).await {
                        log::warn!("Client read loop errored: {e}");
                    }

                    if s.sessions.deregister(public_key, conid).await {
                        log::debug!("Deregistered last connection for {pubkey_string}");

                        if let Some(channel_id) = s.voice.remove(public_key).await {
                            log::info!(
                                "User left voice after disconnect: {pubkey_string} ({channel_id})"
                            );

                            s.sessions
                                .broadcast(&ClientMethod::UserLeftVoice {
                                    channel_id,
                                    pubkey: pubkey_string.clone(),
                                })
                                .await
                                .ok();
                        }
                    }

                    log::info!("Client disconnected: {pubkey_string}");
                }

                Err(e) => {
                    log::warn!("Failed to initialize client: {e}")
                }
            }
        })
    }
}