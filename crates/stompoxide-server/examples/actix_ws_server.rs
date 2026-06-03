use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use actix_web::{App, HttpRequest, HttpResponse, HttpServer, Responder, web};
use actix_ws::Message;
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::StreamExt;
use stompoxide_server::{StompConnectionService, StompServer};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
};
use tower::ServiceExt;

struct WsStream {
    rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Message>,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl WsStream {
    fn new(rx: mpsc::Receiver<Message>, tx: mpsc::Sender<Message>) -> Self {
        Self {
            rx,
            tx,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
        }
    }
}

impl AsyncRead for WsStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if !self.read_buf.is_empty() {
                let len = std::cmp::min(self.read_buf.len(), buf.remaining());
                let data = self.read_buf.split_to(len);
                buf.put_slice(&data);
                return Poll::Ready(Ok(()));
            }

            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(msg)) => match msg {
                    Message::Text(s) => {
                        let bytes = Bytes::copy_from_slice(s.as_bytes());
                        if !bytes.is_empty() {
                            self.read_buf.put_slice(&bytes);
                        }
                    }
                    Message::Binary(b) => {
                        if !b.is_empty() {
                            self.read_buf.put_slice(&b);
                        }
                    }
                    Message::Close(_) => return Poll::Ready(Ok(())), // EOF
                    _ => continue,
                },
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for WsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().write_buf.put_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.write_buf.is_empty() {
            return Poll::Ready(Ok(()));
        }

        let has_null = self.write_buf.contains(&0);
        let is_heartbeat = self.write_buf.iter().all(|&b| b == b'\r' || b == b'\n');

        if has_null || is_heartbeat {
            let data = self.write_buf.split().freeze();
            let msg = if let Ok(s) = std::str::from_utf8(&data) {
                Message::Text(s.to_string().into())
            } else {
                Message::Binary(data)
            };
            if let Err(e) = self.tx.try_send(msg) {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::WriteZero, e)));
            }
        }

        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let _ = self.tx.try_send(Message::Close(None));
        Poll::Ready(Ok(()))
    }
}

async fn ws_handler(
    req: HttpRequest,
    body: web::Payload,
    service: web::Data<StompConnectionService>,
) -> impl Responder {
    let (response, mut session, mut stream) =
        match actix_ws::handle_with_protocols(&req, body, &["v12.stomp", "v11.stomp", "v10.stomp"])
        {
            Ok(res) => res,
            Err(err) => return HttpResponse::InternalServerError().body(err.to_string()),
        };

    let (tx_read, rx_read) = mpsc::channel(128);
    let (tx_write, mut rx_write) = mpsc::channel(128);

    let ws_stream = WsStream::new(rx_read, tx_write);
    let service = service.get_ref().clone();

    // Spawn the Stomp connection handler on the tokio threadpool
    tokio::spawn(async move {
        let _ = service.oneshot(ws_stream).await;
    });

    // Actix Local Task to forward messages to/from WebSocket and the channels
    actix_web::rt::spawn(async move {
        log::info!("Actix WebSocket forwarding loop started");
        loop {
            tokio::select! {
                // Incoming WebSocket messages from client -> forward to tx_read
                msg = stream.next() => {
                    match msg {
                        Some(Ok(msg)) => {
                            log::debug!("Actix WS received msg: {:?}", msg);
                            match msg {
                                Message::Ping(bytes) => {
                                    let _ = session.pong(&bytes).await;
                                }
                                Message::Close(reason) => {
                                    log::info!("Actix WS received Close frame: {:?}", reason);
                                    let _ = tx_read.send(Message::Close(reason.clone())).await;
                                    let _ = session.close(reason).await;
                                    break;
                                }
                                other => {
                                    if tx_read.send(other).await.is_err() {
                                        log::warn!("tx_read send failed, breaking loop");
                                        break;
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            log::error!("Actix WS stream error: {:?}", e);
                            break;
                        }
                        None => {
                            log::info!("Actix WS stream closed (None)");
                            break;
                        }
                    }
                }
                // Outgoing messages from server -> send to client via session
                msg = rx_write.recv() => {
                    match msg {
                        Some(Message::Text(text)) => {
                            if session.text(text).await.is_err() {
                                log::warn!("session.text failed, breaking loop");
                                break;
                            }
                        }
                        Some(Message::Binary(bin)) => {
                            if session.binary(bin).await.is_err() {
                                log::warn!("session.binary failed, breaking loop");
                                break;
                            }
                        }
                        Some(Message::Ping(bytes)) => {
                            if session.ping(&bytes).await.is_err() {
                                log::warn!("session.ping failed, breaking loop");
                                break;
                            }
                        }
                        Some(Message::Pong(bytes)) => {
                            if session.pong(&bytes).await.is_err() {
                                log::warn!("session.pong failed, breaking loop");
                                break;
                            }
                        }
                        Some(Message::Close(reason)) => {
                            log::info!("Sending Close frame to client: {:?}", reason);
                            let _ = session.close(reason).await;
                            break;
                        }
                        None => {
                            log::info!("rx_write closed (None), breaking loop");
                            break;
                        }
                        _ => {
                            log::warn!("Received unhandled message from server, breaking loop");
                            break;
                        }
                    }
                }
            }
        }
        log::info!("Actix WebSocket forwarding loop ended");
    });

    response
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let server = StompServer::new();
    let service = server.connection_service();
    let service_data = web::Data::new(service);

    let host = "127.0.0.1";
    let port = 3000;
    println!(
        "Starting Actix STOMP WebSocket Server on ws://{}:{}/ws...",
        host, port
    );

    HttpServer::new(move || {
        App::new()
            .app_data(service_data.clone())
            .route("/ws", web::get().to(ws_handler))
    })
    .bind((host, port))?
    .run()
    .await
}
