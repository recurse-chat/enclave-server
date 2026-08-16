use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::extract::ws::WebSocket;
use ed25519_dalek::{Signer, VerifyingKey};

use crate::server::Server;

use super::*;
use crate::server::UserConnections;

impl UserConnections {
    pub async fn initialize(
        server: &Arc<Server>,
        mut socket: WebSocket,
    ) -> anyhow::Result<(WebSocket, VerifyingKey, ClientMeta)> {
        let Some(ServerMethod::Initialize {
            public_key: public_key_string,
            signature,

            timestamp,
            hostname,
        }) = read_socket(&mut socket).await?
        else {
            send_socket(
                &mut socket,
                &ClientMethod::Error {
                    error: Cow::Borrowed("Initialization required"),
                },
            )
            .await?;

            return Err(anyhow::anyhow!(
                "Failed to initialize: Client sent the wrong method"
            ));
        };

        let server_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

        if server_timestamp.saturating_sub(timestamp) > 2000 {
            send_socket(
                &mut socket,
                &ClientMethod::Error {
                    error: Cow::Borrowed(
                        "Timestamp doesn't match, make sure it's in secs and is (<= 2secs)",
                    ),
                },
            )
            .await?;

            return Err(anyhow::anyhow!("Client tampstamp wasn't correct"));
        }

        if hostname != server.config.public_hostname || !server.config.hostnames.contains(&hostname)
        {
            send_socket(
                &mut socket,
                &ClientMethod::Error {
                    error: Cow::Owned(format!("Invalid Hostname, to avoid man-in-the-middle attacks, please use the correct hostname: {}", server.config.public_hostname)),
                },
            )
            .await?;

            return Err(anyhow::anyhow!("Client's hostname wasn't correct"));
        }

        let Ok(public_key) = crate::signature::from_string(&public_key_string) else {
            send_socket(
                &mut socket,
                &ClientMethod::Error {
                    error: Cow::Borrowed("Invalid public key"),
                },
            )
            .await?;

            return Err(anyhow::anyhow!("Invalid public key"));
        };

        if public_key
            .verify_strict(
                format!("{timestamp}@{hostname}").as_bytes(),
                &crate::signature::from_string_sig(&signature)?,
            )
            .is_err()
        {
            send_socket(
                &mut socket,
                &ClientMethod::Error {
                    error: Cow::Borrowed("Invalid signature"),
                },
            )
            .await?;

            return Err(anyhow::anyhow!("Invalid signature"));
        }

        {
            send_socket(
                &mut socket,
                &ClientMethod::Initialized {
                    public_key: crate::signature::to_string(&server.key.verifying_key()),
                    signature: server
                        .key
                        .sign(format!("{timestamp}@{hostname}@{public_key_string}").as_bytes())
                        .to_string(),

                    timestamp: server_timestamp,
                    hostname,
                },
            )
            .await?;
        }

        let Some(ServerMethod::Meta(meta)) = read_socket(&mut socket).await? else {
            send_socket(
                &mut socket,
                &ClientMethod::Error {
                    error: Cow::Borrowed("Expected meta"),
                },
            )
            .await?;

            return Err(anyhow::anyhow!(
                "Expected meta, client called another method"
            ));
        };

        Ok((socket, public_key, meta))
    }
}
