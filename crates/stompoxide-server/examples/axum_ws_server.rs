use axum::Router;
use stompoxide_server::StompServer;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let server = StompServer::new();
    let app = Router::new().route_service("/ws", server.websocket_service());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    println!("Starting Axum STOMP WebSocket Server on ws://127.0.0.1:3000/ws...");
    axum::serve(listener, app).await.unwrap();
}
