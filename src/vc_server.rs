use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use ed25519_dalek::VerifyingKey;
use tokio::net::UdpSocket;

use crate::server::Server;

impl Server {
    pub async fn start_udp_server(self: Arc<Self>) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(("0.0.0.0", self.config.port)).await?;

        self.voice_socket
            .set(socket)
            .map_err(|_| anyhow::anyhow!("UDP server already started"))?;

        let mut buf = [0u8; 4096];

        println!("UDP server started");

        loop {
            let (len, addr) = self.get_voice_socket()?.recv_from(&mut buf).await?;

            if len < 8 {
                continue; // too short to even contain a pincode, drop silently
            }

        let pin_bytes: [u8; 8] = buf[..8].try_into().unwrap();
        let pin = u64::from_be_bytes(pin_bytes);

        // First packet carries the pin for authentication; subsequent packets
        // are raw audio identified by UDP address alone.
        let (sender_pubkey, channel_id, payload) = {
            let mut pins = self.voice_pins.lock().await;

            if let Some((pubkey, channel_id)) = pins.remove(&pin) {
                // First packet: pin consumed, strip 8-byte prefix
                (pubkey, channel_id, &buf[8..len])
            } else {
                drop(pins);

                // Not a first-time pin — match by address
                match self.find_voice_sender(&addr).await {
                    Some((pubkey, channel_id)) => (pubkey, channel_id, &buf[..]),
                    None => continue,
                }
            }
        };

            let clients = self.clients.lock().await;
            let Some(user) = clients.get(&sender_pubkey).cloned() else {
                continue; // pin referenced a user that's since disconnected
            };
            drop(clients);

            // Record/refresh this user's known voice address + channel.
            *user.voice.lock().await = Some((addr, channel_id.clone()));

            self.relay_voice(&sender_pubkey, &channel_id, payload).await;
        }
    }

    /// Looks up which known voice participant a UDP address belongs to,
    /// for packets arriving after the initial pin-bearing packet.
    async fn find_voice_sender(&self, addr: &SocketAddr) -> Option<(VerifyingKey, String)> {
        let clients = self.clients.lock().await;

        for (pubkey, user) in clients.iter() {
            if let Some((user_addr, channel)) = &*user.voice.lock().await {
                if *user_addr == *addr {
                    return Some((*pubkey, channel.clone()));
                }
            }
        }

        None
    }

    /// Sends `payload` to every voice participant currently in `channel_id`.
    async fn relay_voice(&self, _sender: &VerifyingKey, channel_id: &str, payload: &[u8]) {
        let clients = self.clients.lock().await;

        for (_pubkey, user) in clients.iter() {
            // if pubkey == sender {
            //     continue;
            // }

            let Some((addr, channel)) = &*user.voice.lock().await else {
                continue;
            };

            if channel_id != channel {
                continue;
            }

            let _ = self.udp_send_to(addr, payload).await;
        }
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
