use axum::extract::ws::WebSocket;

use super::*;

impl super::Client {
    pub async fn initialize(mut socket: WebSocket) -> anyhow::Result<Self> {
        let Some(ServerMethod::Initialize {
            public_key,
            timestamp,
            signature,
        }) = Client::read_socket(&mut socket).await?
        else {
            Client::send_socket(
                &mut socket,
                ClientMethod::Error {
                    error: Cow::Borrowed(
                        "Failed to initialize, unexpected method, expected: initialize",
                    ),
                },
            )
            .await?;

            return Err(anyhow::anyhow!(
                "Failed to initialize: Client sent the wrong method"
            ));
        };

        Client::send_socket(
            &mut socket,
            ClientMethod::Initialized {
                public_key,
                timestamp,
                signature,
            },
        )
        .await?;

        Ok(Self {
            socket,
            meta: super::ClientMeta {},
            public_key: String::new(),
        })
    }
}
