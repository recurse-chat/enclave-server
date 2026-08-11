use axum::extract::ws::WebSocket;

pub mod initialize;

pub struct Client {
    pub socket: WebSocket,
    pub meta: ClientMeta,
    pub pub_key: String,
}

pub struct ClientMeta {}

pub enum ClientMessage {}

pub enum ServerMessage {}

impl Client {
    pub async fn read_socket(mut socket: WebSocket) -> anyhow::Result<ClientMessage> {
        unimplemented!()
        // Ok(socket.recv().await?)
    }
}
