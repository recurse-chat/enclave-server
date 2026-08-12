pub mod protocol;

use axum::{
    Router,
    extract::{WebSocketUpgrade, ws::WebSocket},
    response::Response,
    routing::{any, get},
};

use crate::protocol::Client;

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/meta", get(|| async { "Hello, World!" }))
        .route("/", any(ws_handler));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(|socket: WebSocket| async {
        match Client::initialize(socket).await {
            Ok(mut client) => {
                if let Err(e) = client.read_loop().await {
                    eprintln!("Failed to handle client: {e}");
                } else {
                    println!("Client connection closed")
                }
            }

            Err(e) => {
                eprintln!("Failed to initialize client: {e}")
            }
        }
    })
}
