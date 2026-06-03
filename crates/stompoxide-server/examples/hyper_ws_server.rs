use hyper_util::{rt::TokioIo, server::conn::auto, service::TowerToHyperService};
use stompoxide_server::StompServer;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let server = StompServer::new();
    let service = server.websocket_service();

    let addr = "127.0.0.1:3000";
    let listener = TcpListener::bind(addr).await?;
    println!(
        "Starting Hyper STOMP WebSocket Server on ws://{}/ws...",
        addr
    );

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let service = service.clone();

        tokio::spawn(async move {
            let conn = auto::Builder::new(hyper_util::rt::TokioExecutor::new());
            let srv = conn.serve_connection_with_upgrades(io, TowerToHyperService::new(service));
            if let Err(err) = srv.await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
