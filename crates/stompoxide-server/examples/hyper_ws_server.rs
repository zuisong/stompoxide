use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{Sink, Stream};
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming, service::service_fn};
use hyper_tungstenite::{WebSocketStream, is_upgrade_request, tungstenite::Message, upgrade};
use hyper_util::{rt::TokioIo, server::conn::auto};
use stompoxide_server::{StompConnectionService, StompServer};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
};
use tower::ServiceExt;

struct WsStream {
    ws: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl WsStream {
    fn new(ws: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>) -> Self {
        Self {
            ws,
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

            match Pin::new(&mut self.ws).poll_next(cx) {
                Poll::Ready(Some(Ok(msg))) => match msg {
                    Message::Text(s) => {
                        let bytes = Bytes::copy_from_slice(s.as_str().as_bytes());
                        if !bytes.is_empty() {
                            self.read_buf.put_slice(&bytes);
                        }
                    }
                    Message::Binary(b) => {
                        if !b.is_empty() {
                            self.read_buf.put_slice(&b);
                        }
                    }
                    Message::Ping(_) | Message::Pong(_) => {
                        continue;
                    }
                    Message::Close(_) => return Poll::Ready(Ok(())), // EOF
                    _ => continue,
                },
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, e)));
                }
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl AsyncWrite for WsStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write_buf.put_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.write_buf.is_empty() {
            return Poll::Ready(Ok(()));
        }

        let has_null = self.write_buf.contains(&0);
        let is_heartbeat = self.write_buf.iter().all(|&b| b == b'\r' || b == b'\n');

        if has_null || is_heartbeat {
            match Pin::new(&mut self.ws).poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    let data = self.write_buf.split().freeze();
                    let msg = if let Ok(s) = std::str::from_utf8(&data) {
                        Message::Text(s.to_string().into())
                    } else {
                        Message::Binary(data)
                    };
                    if let Err(e) = Pin::new(&mut self.ws).start_send(msg) {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::WriteZero, e)));
                    }
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, e)));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        match Pin::new(&mut self.ws).poll_flush(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.ws).poll_close(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

async fn handle_request(
    mut req: Request<Incoming>,
    service: StompConnectionService,
) -> Result<Response<Full<Bytes>>, io::Error> {
    if is_upgrade_request(&req) {
        let selected_protocol = {
            let requested_protocol = req
                .headers()
                .get("Sec-WebSocket-Protocol")
                .and_then(|h| h.to_str().ok());

            let client_protocols: Vec<&str> = requested_protocol
                .unwrap_or("")
                .split(',')
                .map(|p| p.trim())
                .collect();

            if client_protocols.contains(&"v12.stomp") {
                "v12.stomp".to_string()
            } else if client_protocols.contains(&"v11.stomp") {
                "v11.stomp".to_string()
            } else if client_protocols.contains(&"v10.stomp") {
                "v10.stomp".to_string()
            } else {
                "v12.stomp".to_string()
            }
        };

        let (mut response, websocket) =
            upgrade(&mut req, None).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        response
            .headers_mut()
            .insert("Sec-WebSocket-Protocol", selected_protocol.parse().unwrap());

        tokio::spawn(async move {
            if let Ok(ws) = websocket.await {
                let stream = WsStream::new(ws);
                let _ = service.oneshot(stream).await;
            }
        });

        Ok(response)
    } else {
        Ok(Response::new(Full::new(Bytes::from(
            "Not a websocket request",
        ))))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let server = StompServer::new();
    let service = server.connection_service();

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
            let srv = conn.serve_connection_with_upgrades(
                io,
                service_fn(move |req| handle_request(req, service.clone())),
            );
            if let Err(err) = srv.await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
