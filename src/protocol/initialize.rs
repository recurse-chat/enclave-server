use axum::extract::ws::WebSocket;

use super::*;

impl super::Client {
    pub async fn initialize(mut socket: WebSocket) -> anyhow::Result<Self> {
        if let Some(ServerMessage::Initialize()) = Client::read_socket(&mut socket).await? {}

        Ok(Self {
            socket,
            meta: super::ClientMeta {},
            pub_key: String::new(),
        })
    }
}
