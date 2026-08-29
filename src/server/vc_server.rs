use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use anyhow::Context;
use ed25519_dalek::VerifyingKey;
use tokio::{
    net::UdpSocket,
    sync::{Mutex, OnceCell},
    time::Instant,
};

use crate::{
    crypto::SessionCipher,
    protocol::ClientMethod,
    server::UserConnections,
};

pub struct VoicePin {
    pub pubkey: VerifyingKey,
    pub channel_id: String,
}

pub struct VoiceConnection {
    pub user: Arc<UserConnections>,
    pub channel_id: String,
    pub addr: SocketAddr,
    pub last_speaking_sent: Instant,
}

pub struct VoiceServer {
    pub pins: Mutex<HashMap<u64, VoicePin>>,
    pub socket: OnceCell<UdpSocket>,
    pub participants: Mutex<HashMap<VerifyingKey, VoiceConnection>>,
}

impl Default for VoiceServer {
    fn default() -> Self {
        Self::new()
    }
}

impl VoiceServer {
    pub fn new() -> Self {
        Self {
            pins: Mutex::new(HashMap::new()),
            socket: OnceCell::new(),
            participants: Mutex::new(HashMap::new()),
        }
    }

    /// Adds a user to a voice channel and allocates a one-time pin that
    /// their next UDP packet must carry to bind their address.
    pub async fn join(
        &self,
        public_key: VerifyingKey,
        user: Arc<UserConnections>,
        channel_id: &str,
    ) -> u64 {
        let pin = rand::random::<u64>() % (1 << 53);

        self.participants.lock().await.insert(
            public_key,
            VoiceConnection {
                user,
                channel_id: channel_id.to_string(),
                addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                last_speaking_sent: Instant::now(),
            },
        );

        self.pins.lock().await.insert(
            pin,
            VoicePin {
                pubkey: public_key,
                channel_id: channel_id.to_string(),
            },
        );

        pin
    }

    /// Removes a user from every voice channel/pin they hold, returning
    /// the channel they were in, if any.
    pub async fn remove(&self, public_key: VerifyingKey) -> Option<String> {
        let channel_id = self
            .participants
            .lock()
            .await
            .remove(&public_key)?
            .channel_id;

        self.pins.lock().await.retain(|_, pin| pin.pubkey != public_key);

        Some(channel_id)
    }

    /// Runs the UDP loop that terminates voice audio: binds addresses on
    /// the first (pin-bearing) packet and relays encrypted audio between
    /// the participants of each channel.
    pub async fn run(self: Arc<Self>, port: u16) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(("0.0.0.0", port)).await?;

        self.socket
            .set(socket)
            .map_err(|_| anyhow::anyhow!("UDP server already started"))?;

        let mut buf = [0u8; 4096];

        eprintln!("[vc] UDP server listening on port {port}");

        loop {
            let (len, addr) = self.get_voice_socket()?.recv_from(&mut buf).await?;

            if len < 8 {
                eprintln!("[vc] dropping packet too short for pin");
                continue;
            }

            let pin_bytes: [u8; 8] = buf[..8].try_into().unwrap();
            let pin = u64::from_be_bytes(pin_bytes);

            // is_first_packet distinguishes the plaintext pin-bootstrap packet
            // from subsequent encrypted audio packets.
            let (sender_pubkey, channel_id, payload, is_first_packet) = {
                let mut pins = self.pins.lock().await;

                if let Some(pin) = pins.remove(&pin) {
                    (pin.pubkey, pin.channel_id, &buf[8..len], true)
                } else {
                    drop(pins);

                    match self.find_sender(&addr).await {
                        Some((pubkey, channel_id)) => (pubkey, channel_id, &buf[..len], false),
                        None => continue,
                    }
                }
            };

            if let Some(participant) = self.participants.lock().await.get_mut(&sender_pubkey) {
                participant.addr = addr;
            }

            // The pin-bearing bootstrap packet carries no payload to decrypt —
            // it's purely "here's my pin, bind my address."
            if is_first_packet {
                continue;
            }

            let Some(cipher) = ({
                let participants = self.participants.lock().await;
                participants.get(&sender_pubkey).map(|p| p.user.cipher.clone())
            }) else {
                eprintln!("[vc] sender pubkey not found in participants, dropping");
                continue;
            };

            let decrypted_payload = match cipher.lock().await.decrypt(payload) {
                Ok(pt) => pt,
                Err(e) => {
                    eprintln!("[vc] dropping packet: decryption failed: {e}");
                    continue;
                }
            };

            let s = self.clone();

            tokio::spawn(async move {
                if let Err(e) = s
                    .relay_voice(&sender_pubkey, &channel_id, &decrypted_payload)
                    .await
                {
                    eprintln!("{e}");
                }
            });
        }
    }

    /// Looks up which known voice participant a UDP address belongs to,
    /// for packets arriving after the initial pin-bearing packet.
    async fn find_sender(&self, addr: &SocketAddr) -> Option<(VerifyingKey, String)> {
        let participants = self.participants.lock().await;

        for (pubkey, participant) in participants.iter() {
            if *addr == participant.addr {
                return Some((*pubkey, participant.channel_id.clone()));
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
        let mut participants = self.participants.lock().await;

        for (_pubkey, participant) in participants.iter_mut() {
            if channel_id != participant.channel_id {
                continue;
            }

            let now = Instant::now();

            if now.duration_since(participant.last_speaking_sent).as_millis() >= 600 {
                participant
                    .user
                    .send(&ClientMethod::Speaking {
                        pubkey: crate::crypto::to_string(sender),
                    })
                    .await?;

                participant.last_speaking_sent = now;
            }

            if *sender == participant.user.public_key {
                continue;
            }

            let _ = self
                .udp_send_to(&participant.user.cipher, &participant.addr, payload)
                .await;
        }

        Ok(())
    }

    pub fn get_voice_socket(&self) -> anyhow::Result<&UdpSocket> {
        self.socket
            .get()
            .context("Failed to get voice socket")
    }

    pub async fn udp_send_to(
        &self,
        cipher: &Arc<Mutex<SessionCipher>>,
        addr: &SocketAddr,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        let socket = self.get_voice_socket()?;

        socket
            .send_to(&cipher.lock().await.encrypt(payload)?, addr)
            .await?;

        Ok(())
    }
}