use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{BufMut, BytesMut};
use futures_util::{Sink, Stream};
use stompoxide_server::{StompConnectionService, StompServer};
use tokio::io::{AsyncRead, AsyncWrite};
use tower::ServiceExt;
use warp::{
    Filter,
    ws::{Message, WebSocket},
};

struct WsStream {
    ws: WebSocket,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl WsStream {
    fn new(ws: WebSocket) -> Self {
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
                Poll::Ready(Some(Ok(msg))) => {
                    if msg.is_text() || msg.is_binary() {
                        let bytes = msg.as_bytes();
                        if !bytes.is_empty() {
                            self.read_buf.put_slice(bytes);
                        }
                    } else if msg.is_close() {
                        return Poll::Ready(Ok(())); // EOF
                    }
                }
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
                        Message::text(s)
                    } else {
                        Message::binary(data)
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
                    let stream = WsStream::new(socket);
                    let _ = service.oneshot(stream).await;
                });

                // Negotiate Sec-WebSocket-Protocol header, prioritizing higher versions
                let client_protocols: Vec<&str> = protocol
                    .as_deref()
                    .unwrap_or("")
                    .split(',')
                    .map(|p| p.trim())
                    .collect();

                let selected_protocol = if client_protocols.contains(&"v12.stomp") {
                    "v12.stomp"
                } else if client_protocols.contains(&"v11.stomp") {
                    "v11.stomp"
                } else if client_protocols.contains(&"v10.stomp") {
                    "v10.stomp"
                } else {
                    "v12.stomp"
                };

                warp::reply::with_header(reply, "sec-websocket-protocol", selected_protocol)
            },
        );

    let addr = ([127, 0, 0, 1], 3000);
    println!("Starting Warp STOMP WebSocket Server on ws://127.0.0.1:3000/ws...");
    warp::serve(routes).run(addr).await;
}
