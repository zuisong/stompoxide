# stompoxide-client

`stompoxide-client` is an async STOMP client built on Tokio streams.

It can connect over any stream that implements `AsyncRead + AsyncWrite`, which
makes it usable with TCP sockets and adapted WebSocket streams.

## Example

```rust
use futures_util::StreamExt;
use stompoxide_client::{AckMode, ClientConfig, SendRequest, StompClient, SubscribeRequest};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:61613").await?;
    let config = ClientConfig {
        host: "localhost".to_string(),
        heartbeat_cx: 5000,
        heartbeat_cy: 5000,
        ..ClientConfig::default()
    };

    let (client, _handle) = StompClient::connect(stream, config).await?;
    let (sender, subscriber) = client.split();
    let mut subscription = subscriber
        .subscribe(
            SubscribeRequest::new("/queue/demo")
                .id("worker-1")
                .ack(AckMode::ClientIndividual),
        )
        .await?;

    sender
        .send(
            SendRequest::new("/queue/demo", "hello")
                .headers(vec![("content-type".to_string(), "text/plain".to_string())])
                .receipt("send-1"),
        )
        .await?;

    if let Some(frame) = subscription.next().await {
        println!("received frame: {:?}", frame.command);
    }

    Ok(())
}
```

## Behavior

- Sends a STOMP `CONNECT` frame during connection setup.
- Negotiates incoming and outgoing heartbeats from the `CONNECTED` frame.
- Sends raw EOL heartbeats and flushes the stream.
- Ignores heartbeat frames while waiting for the initial `CONNECTED` frame.
- Can split into a `StompSender` for sending and a `StompSubscriber` for
  creating subscription streams.
- Sends messages with `SendRequest`, including custom headers and receipt
  headers.
- Subscribes with `SubscribeRequest`, including custom ids, headers, and
  `AckMode`.
- Exposes each subscription as an async stream of `MESSAGE` frames.
- Sends `UNSUBSCRIBE` when a subscription is dropped.

`send(...).await` confirms that the frame was written to the connection task.
It does not currently wait for a broker `RECEIPT` frame, even when the request
includes a receipt header.
