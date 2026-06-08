use bytes::Bytes;
use stompoxide_server::{StompConnectionService, StompServer, select_stomp_subprotocol};
use tower::ServiceExt;
use warp::{
    Filter,
    ws::{Message, WebSocket},
};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let server = StompServer::new();
    let service = server.connection_service();

    let routes = warp::path("ws")
        .and(warp::ws())
        .and(warp::header::optional::<String>("sec-websocket-protocol"))
        .and(warp::any().map(move || service.clone()))
        .map(
            |ws: warp::ws::Ws, protocol: Option<String>, service: StompConnectionService| {
                let reply = ws.on_upgrade(move |socket| async move {
                    let stream = Box::pin(websocket_io(socket));
                    let _ = service.oneshot(stream).await;
                });

                let selected_protocol = select_stomp_subprotocol(protocol.as_deref());

                warp::reply::with_header(reply, "sec-websocket-protocol", selected_protocol)
            },
        );

    let addr = ([127, 0, 0, 1], 3000);
    println!("Starting Warp STOMP WebSocket Server on ws://127.0.0.1:3000/ws...");
    warp::serve(routes).run(addr).await;
}

fn websocket_io(
    websocket: WebSocket,
) -> impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static {
    stompoxide_server::ws::websocket_io(
        websocket,
        |message| {
            if message.is_text() || message.is_binary() {
                let bytes = Bytes::copy_from_slice(message.as_bytes());
                if bytes.is_empty() { None } else { Some(bytes) }
            } else {
                None
            }
        },
        |bytes| {
            if let Ok(text) = std::str::from_utf8(&bytes) {
                Message::text(text)
            } else {
                Message::binary(bytes)
            }
        },
    )
}
