use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU16},
};

use ed25519_dalek::VerifyingKey;
use tokio::sync::Mutex;

use crate::{
    crypto::SessionCipher,
    protocol::ClientMethod,
    types::ClientMeta,
    ws::EnclaveWebSocket,
};

pub struct UserConnections {
    pub meta: ClientMeta,
    pub counter: AtomicU16,
    pub public_key: VerifyingKey,
    pub connections: Mutex<HashMap<u16, Arc<EnclaveWebSocket>>>,
    pub cipher: Arc<Mutex<SessionCipher>>,
}

pub struct SessionRegistry {
    pub clients: Mutex<HashMap<VerifyingKey, Arc<UserConnections>>>,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionRegistry {
    pub fn new() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, public_key: &VerifyingKey) -> Option<Arc<UserConnections>> {
        self.clients.lock().await.get(public_key).cloned()
    }

    /// Registers a new websocket connection for a user, returning the
    /// connection and its id. The websocket is given the shared cipher
    /// of the user's existing connections so voice traffic stays keyed
    /// consistently across devices.
    pub async fn register(
        &self,
        public_key: VerifyingKey,
        meta: ClientMeta,
        mut client: EnclaveWebSocket,
    ) -> (Arc<EnclaveWebSocket>, u16) {
        let mut clients = self.clients.lock().await;

        let user = clients
            .entry(public_key)
            .or_insert_with(|| {
                Arc::new(UserConnections {
                    meta,
                    public_key,
                    counter: AtomicU16::new(0),
                    connections: Mutex::new(HashMap::new()),
                    cipher: client.cipher.clone(),
                })
            })
            .clone();

        client.cipher = user.cipher.clone();

        let client = Arc::new(client);

        let conid = user.counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        user.connections.lock().await.insert(conid, client.clone());

        log::debug!(
            "Registered connection {conid} for user {}",
            crate::crypto::to_string(&public_key)
        );

        (client, conid)
    }

    /// Removes a connection from a user's session. Returns `true` when
    /// that was the user's last connection and they have been dropped
    /// from the registry entirely.
    pub async fn deregister(&self, public_key: VerifyingKey, conid: u16) -> bool {
        let mut clients = self.clients.lock().await;

        let Some(user) = clients.get(&public_key).cloned() else {
            return false;
        };

        let mut connections = user.connections.lock().await;
        connections.remove(&conid);

        log::debug!(
            "Deregistered connection {conid} for user {}",
            crate::crypto::to_string(&public_key)
        );

        if connections.is_empty() {
            clients.remove(&public_key);
            true
        } else {
            false
        }
    }

    pub async fn broadcast(&self, message: &ClientMethod) -> anyhow::Result<()> {
        let users: Vec<_> = self.clients.lock().await.values().cloned().collect();

        for user in users {
            if let Err(e) = user.send(message).await {
                log::warn!("Failed to send to a client: {e:?}");
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