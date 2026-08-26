use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use ed25519_dalek::VerifyingKey;
use tokio::net::UdpSocket;

use crate::{
    protocol::{ClientMethod, send_socket},
    server::Server,
};

use tokio::time::Instant;

impl Server {
    pub async fn start_udp_server(self: Arc<Self>) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(("0.0.0.0", self.config.port)).await?;

        self.voice_socket
            .set(socket)
            .map_err(|_| anyhow::anyhow!("UDP server already started"))?;

        let mut buf = [0u8; 4096];

        eprintln!("[vc] UDP server listening on port {}", self.config.port);

        loop {
            let (len, addr) = self.get_voice_socket()?.recv_from(&mut buf).await?;

            if len < 8 {
                eprintln!("[vc] dropping packet too short for pin");
                continue;
            }

            let pin_bytes: [u8; 8] = buf[..8].try_into().unwrap();
            let pin = u64::from_be_bytes(pin_bytes);

            let (sender_pubkey, channel_id, payload) = {
                let mut pins = self.voice_pins.lock().await;

                if let Some((pubkey, channel_id)) = pins.remove(&pin) {
                    (pubkey, channel_id, &buf[8..len])
                } else {
                    drop(pins);

                    match self.find_voice_sender(&addr).await {
                        Some((pubkey, channel_id)) => (pubkey, channel_id, &buf[..]),
                        None => {
                            continue;
                        }
                    }
                }
            };

            let clients = self.clients.lock().await;
            let Some(user) = clients.get(&sender_pubkey).cloned() else {
                eprintln!("[vc] sender pubkey not found in clients, dropping");
                continue;
            };
            drop(clients);

            *user.voice.lock().await = Some(crate::server::VoiceConnection {
                addr,
                channel_id: channel_id.clone(),
                last_speaking_sent: Instant::now(),
            });

            self.relay_voice(&sender_pubkey, &channel_id, payload)
                .await?;
        }
    }

    /// Looks up which known voice participant a UDP address belongs to,
    /// for packets arriving after the initial pin-bearing packet.
    async fn find_voice_sender(&self, addr: &SocketAddr) -> Option<(VerifyingKey, String)> {
        let clients = self.clients.lock().await;

        for (pubkey, user) in clients.iter() {
            if let Some(voice) = &*user.voice.lock().await {
                if *addr == voice.addr {
                    return Some((*pubkey, voice.channel_id.clone()));
                }
            }
        }

        None
    }

    /// Sends `payload` to every voice participant currently in `channel_id`.
    async fn relay_voice(
        &self,
        sender: &VerifyingKey,
        channel_id: &str,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let clients = self.clients.lock().await;

        for (_pubkey, user) in clients.iter() {
            let Some(voice) = &mut *user.voice.lock().await else {
                continue;
            };

            if channel_id != voice.channel_id {
                continue;
            }

            let now = Instant::now();

            if now.duration_since(voice.last_speaking_sent).as_millis() >= 1000 {
                for conn in user.connections.lock().await.values() {
                    let _ = send_socket(
                        &mut *conn.lock().await,
                        &ClientMethod::Speaking {
                            pubkey: crate::crypto::to_string(sender),
                        },
                    )
                    .await;
                }
            }

            voice.last_speaking_sent = now;

            let _ = self.udp_send_to(&voice.addr, payload).await;
        }

        Ok(())
    }

    pub fn get_voice_socket(&self) -> anyhow::Result<&UdpSocket> {
        self.voice_socket
            .get()
            .context("Failed to get voice socket")
    }

    pub async fn udp_send_to(&self, addr: &SocketAddr, payload: &[u8]) -> anyhow::Result<()> {
        let socket = self.get_voice_socket()?;

        socket.send_to(payload, addr).await?;

        Ok(())
    }
}
