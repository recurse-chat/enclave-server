pub mod protocol;
pub mod server;
pub mod signature;

use std::sync::Arc;

use axum::{
    Router,
    extract::{State, WebSocketUpgrade},
    response::Response,
    routing::{any, get},
};

use crate::server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Server::new().await?;

    let app = Router::new()
        .route("/meta", get(|| async { "Hello, World!" }))
        .route("/", any(ws_handler))
        .with_state(server);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;

    axum::serve(listener, app).await?;

    Ok(())
}

async fn ws_handler(State(server): State<Arc<Server>>, ws: WebSocketUpgrade) -> Response {
    server.ws_handler(ws).await
}
