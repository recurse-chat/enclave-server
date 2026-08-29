use std::{borrow::Cow, collections::HashMap, sync::Arc};

use ed25519_dalek::VerifyingKey;
use serde::{Deserialize, Serialize};

use crate::{data::messages::StoredMessage, server::Server, types::ClientMeta};

pub mod initialize;
pub mod message;
pub mod user;
pub mod voice;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum ClientMethod {
    Initialized {
        public_key: String,
        signature: String,

        timestamp: u64,
        hostname: String,
    },

    Error {
        error: Cow<'static, str>,
    },

    Messages {
        messages: HashMap<String, Vec<StoredMessage>>,
    },

    Users {
        users: HashMap<String, ClientMeta>,
    },

    MessageEdited {
        channel_id: String,
        message: StoredMessage,
    },

    MessageDeleted {
        channel_id: String,
        message_id: String,
    },

    JoinVoice {
        channel_id: String,
        pin: u64,
    },

    UserJoinedVoice {
        channel_id: String,
        pubkey: String,
    },

    UserLeftVoice {
        channel_id: String,
        pubkey: String,
    },

    Speaking {
        pubkey: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum ServerMethod {
    Initialize {
        public_key: String,
        signature: String,

        timestamp: u64,
        hostname: String,
    },

    Error {
        error: String,
    },

    SendMessage {
        channel_id: String,
        data: crate::data::messages::MessageData,
    },

    GetMessages {
        channel_id: String,
        chunk: u32,
    },

    Meta(ClientMeta),

    GetUsers {
        pubkeys: Vec<String>,
    },

    EditMessage {
        message_id: String,
        channel_id: String,
        content: String,
        signature: String,
    },

    DeleteMessage {
        message_id: String,
        channel_id: String,
    },

    JoinVoice {
        channel_id: String,
    },

    LeaveVoice,
}

pub async fn read_loop(
    server: &Arc<Server>,
    verifying_key: VerifyingKey,
    socket: &Arc<crate::ws::EnclaveWebSocket>,
) -> anyhow::Result<()> {
    let pubkey_string = crate::crypto::to_string(&verifying_key);

    while let Some(message) = socket.read().await? {
        match message {
            ServerMethod::Initialize { .. } => {
                log::warn!("Client {pubkey_string} sent Initialize after already initializing");

                socket
                    .send(&ClientMethod::Error {
                        error: Cow::Borrowed("Already initialized"),
                    })
                    .await?;
            }

            #[allow(unused_variables)]
            ServerMethod::Meta(meta) => {}

            ServerMethod::Error { error } => {
                log::warn!("Client error (client {pubkey_string}): {error}");
            }

            ServerMethod::SendMessage { channel_id, data } => {
                message::send_message(server, verifying_key, socket, data, channel_id).await?;
            }

            ServerMethod::GetMessages { channel_id, chunk } => {
                message::get_messages(server, verifying_key, socket, channel_id, chunk).await?;
            }

            ServerMethod::DeleteMessage {
                message_id,
                channel_id,
            } => {
                message::delete_message(server, verifying_key, message_id, channel_id).await?;
            }

            ServerMethod::EditMessage {
                message_id,
                channel_id,
                content,
                signature,
            } => {
                message::edit_message(
                    server,
                    verifying_key,
                    message_id,
                    channel_id,
                    content,
                    signature,
                )
                .await?;
            }

            ServerMethod::GetUsers { pubkeys } => {
                user::get_users(server, verifying_key, socket, pubkeys).await?;
            }

            ServerMethod::JoinVoice { channel_id } => {
                voice::join(server, verifying_key, socket, channel_id).await?;
            }

            ServerMethod::LeaveVoice => {
                voice::leave(server, verifying_key).await?;
            }
        }
    }

    Ok(())
}
