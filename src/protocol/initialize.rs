use std::sync::Arc;

use axum::extract::ws::WebSocket;
use ed25519_dalek::Signer;

use crate::server::Server;

use super::*;

impl super::Client {
    pub async fn initialize(server: &Arc<Server>, mut socket: WebSocket) -> anyhow::Result<Self> {
        let Some(ServerMethod::Initialize {
            public_key: public_key_string,
            timestamp,
            signature,
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
                format!("{public_key_string}@{timestamp}@").as_bytes(),
                &crate::signature::from_string_sig(&signature)?,
            )
            .is_ok()
        {
            return Err(anyhow::anyhow!("Invalid signature"));
        }

        Client::send_socket(
            &mut socket,
            ClientMethod::Initialized {
                public_key: crate::signature::to_string(&server.key.verifying_key()),
                signature: server
                    .key
                    .sign(format!("{public_key_string}@{timestamp}").as_bytes())
                    .to_string(),
            },
        )
        .await?;

        Ok(Self {
            socket,
            meta: super::ClientMeta {},
            public_key,
        })
    }
}
