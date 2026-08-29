pub mod crypto;
pub mod data;
pub mod protocol;
pub mod server;
pub mod types;
pub mod ws;

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
use tower_http::{
    cors::{Any, CorsLayer},
    services::ServeFile,
};

use crate::server::Server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    log::info!("Starting enclave-server v{}", env!("CARGO_PKG_VERSION"));

    let server = Server::new().await?;

    let udp_server = tokio::spawn(server.voice.clone().run(server.config.port));

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/meta", get(meta))
        .route("/", any(ws_handler))
        .route_service("/icon", ServeFile::new("./icon.png"))
        .with_state(server.clone())
        .layer(cors);

    let listener = tokio::net::TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        server.config.port,
    ))
    .await?;

    log::info!("HTTP/WS server listening on 0.0.0.0:{}", server.config.port);

    axum::serve(listener, app).await?;

    log::info!("Shutting down UDP voice server");

    udp_server.abort();

    Ok(())
}

async fn meta(State(server): State<Arc<Server>>) -> String {
    serde_json::to_string(&server.config.meta.clone()).unwrap()
}

async fn ws_handler(State(server): State<Arc<Server>>, ws: WebSocketUpgrade) -> Response {
    server.ws_handler(ws).await
}
