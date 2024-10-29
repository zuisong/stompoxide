use stompoxide_server::StompServer;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    // Initialize tracing/logging
    tracing_subscriber::fmt::init();

    let server = StompServer::new();
    let addr = "127.0.0.1:61613";

    println!("Starting standalone STOMP TCP Broker on {}...", addr);
    server.listen_tcp(addr).await?;

    Ok(())
}
