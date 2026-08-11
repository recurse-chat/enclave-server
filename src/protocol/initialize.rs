use axum::extract::ws::WebSocket;

impl super::Client {
    pub async fn initialize(mut socket: WebSocket) -> anyhow::Result<Self> {
        Ok(Self {
            socket,
            meta: super::ClientMeta {},
            pub_key: String::new(),
        })
    }
}
