use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::extract::ws::WebSocket;
use ed25519_dalek::Signer;

use crate::server::Server;

use super::*;

impl super::Client {
    pub async fn initialize(
        server: &Arc<Server>,
        mut socket: WebSocket,
    ) -> anyhow::Result<(Self, ClientMeta)> {
        let Some(ServerMethod::Initialize {
            public_key: public_key_string,
            signature,

            timestamp,
            hostname,
        }) = Client::read_socket(&mut socket).await?
        else {
            Client::send_socket(
                &mut socket,
                ClientMethod::Error {
                    error: Cow::Borrowed("Initialization required"),
                },
            )
            .await?;

            return Err(anyhow::anyhow!(
                "Failed to initialize: Client sent the wrong method"
            ));
        };

        let public_key = crate::signature::from_string(&public_key_string)?;

        if public_key
            .verify_strict(
                format!("{timestamp}@{hostname}").as_bytes(),
                &crate::signature::from_string_sig(&signature)?,
            )
            .is_ok()
        {
            return Err(anyhow::anyhow!("Invalid signature"));
        }

        {
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

            Client::send_socket(
                &mut socket,
                ClientMethod::Initialized {
                    public_key: crate::signature::to_string(&server.key.verifying_key()),
                    signature: server
                        .key
                        .sign(format!("{timestamp}@{hostname}@{public_key_string}").as_bytes())
                        .to_string(),

                    timestamp,
                    hostname,
                },
            )
            .await?;
        }

        let Some(ServerMethod::Meta(meta)) = Client::read_socket(&mut socket).await? else {
            Client::send_socket(
                &mut socket,
                ClientMethod::Error {
                    error: Cow::Borrowed("Expected meta"),
                },
            )
            .await?;

            return Err(anyhow::anyhow!(
                "Expected meta, client called another method"
            ));
        };

        Ok((
            Self {
                socket: Arc::new(Mutex::new(socket)),
                public_key,
            },
            meta,
        ))
    }
}
