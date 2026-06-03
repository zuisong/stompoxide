use std::{
    convert::Infallible,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::{Request, Response, StatusCode, body::Body};
use hyper_tungstenite::{is_upgrade_request, tungstenite::Message, upgrade};
use tokio::io::join;
use tokio_util::io::{CopyToBytes, SinkWriter, StreamReader};
use tower::Service;

use crate::{StompConnectionService, select_stomp_subprotocol};

#[derive(Clone)]
pub struct StompWebSocketService {
    connection_service: StompConnectionService,
}

impl StompWebSocketService {
    pub fn new(connection_service: StompConnectionService) -> Self {
        Self { connection_service }
    }
}

impl<B> Service<Request<B>> for StompWebSocketService
where
    B: Body + Send + 'static,
{
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        let mut connection_service = self.connection_service.clone();

        Box::pin(async move {
            if !is_upgrade_request(&req) {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from("expected websocket upgrade request")))
                    .expect("valid websocket error response"));
            }

            let selected_protocol = select_stomp_subprotocol(
                req.headers()
                    .get("Sec-WebSocket-Protocol")
                    .and_then(|h| h.to_str().ok()),
            );

            match upgrade(&mut req, None) {
                Ok((mut response, websocket)) => {
                    response.headers_mut().insert(
                        "Sec-WebSocket-Protocol",
                        selected_protocol.parse().expect("valid subprotocol header"),
                    );

                    tokio::spawn(async move {
                        if let Ok(ws) = websocket.await {
                            let stream = Box::pin(websocket_io(ws));
                            let _ = connection_service.call(stream).await;
                        }
                    });

                    Ok(response)
                }
                Err(error) => Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Full::new(Bytes::from(format!(
                        "websocket upgrade failed: {error}"
                    ))))
                    .expect("valid upgrade error response")),
            }
        })
    }
}

fn websocket_io(
    websocket: hyper_tungstenite::WebSocketStream<
        hyper_util::rt::TokioIo<hyper::upgrade::Upgraded>,
    >,
) -> impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static {
    let (write_half, read_half) = websocket.split();

    let reader = StreamReader::new(read_half.filter_map(|message| async move {
        match message {
            Ok(Message::Text(text)) => Some(Ok(Bytes::copy_from_slice(text.as_str().as_bytes()))),
            Ok(Message::Binary(bytes)) if !bytes.is_empty() => Some(Ok(bytes)),
            Ok(Message::Binary(_))
            | Ok(Message::Ping(_))
            | Ok(Message::Pong(_))
            | Ok(Message::Close(_))
            | Ok(Message::Frame(_)) => None,
            Err(error) => Some(Err(io::Error::new(io::ErrorKind::ConnectionReset, error))),
        }
    }));

    let writer = SinkWriter::new(CopyToBytes::new(
        write_half
            .sink_map_err(|error| io::Error::new(io::ErrorKind::ConnectionReset, error))
            .with(
                |bytes: Bytes| async move { Ok::<Message, io::Error>(message_from_bytes(bytes)) },
            ),
    ));

    join(reader, writer)
}

fn message_from_bytes(bytes: Bytes) -> Message {
    if let Ok(text) = std::str::from_utf8(&bytes) {
        Message::Text(text.to_owned().into())
    } else {
        Message::Binary(bytes)
    }
}
