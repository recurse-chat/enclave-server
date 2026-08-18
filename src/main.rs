pub mod config;
pub mod protocol;
pub mod server;
pub mod signature;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use axum::{
    Router,
    extract::{State, WebSocketUpgrade},
    response::Response,
    routing::{any, get},
};
use tower_http::services::ServeFile;

use crate::server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = Server::new().await?;

    let app = Router::new()
        .route("/meta", get(meta))
        .route("/", any(ws_handler))
        .route_service("/icon", ServeFile::new("./icon.png"))
        .with_state(server.clone());

    let listener = tokio::net::TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        server.config.port,
    ))
    .await?;

    axum::serve(listener, app).await?;

    Ok(())
}

async fn meta(State(server): State<Arc<Server>>) -> String {
    serde_json::to_string(&server.config.meta.clone()).unwrap()
}

async fn ws_handler(State(server): State<Arc<Server>>, ws: WebSocketUpgrade) -> Response {
    server.ws_handler(ws).await
}
