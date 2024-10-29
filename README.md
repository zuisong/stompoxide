# Stompoxide

Stompoxide is a Rust workspace for building STOMP-based messaging components.
It currently provides a STOMP frame codec, an async client, and a lightweight
pub/sub server implementation.

The project currently targets STOMP 1.2 connections and focuses on plain async
streams, so the client and server can run over TCP or adapted WebSocket
streams.

## Crates

| Crate | Purpose |
| --- | --- |
| `stompoxide-codec` | Parse, serialize, encode, and decode STOMP frames. |
| `stompoxide-client` | Async STOMP client with connect, send, subscribe, unsubscribe, and heartbeat handling. |
| `stompoxide-server` | Lightweight async STOMP server with in-memory pub/sub routing. |

## Features

- STOMP frame parsing and serialization
- `tokio-util` codec support
- CONNECT / STOMP handshake support
- STOMP 1.2 connection negotiation on the server
- Heartbeat decoding, sending, flushing, and timeout checks
- Client-side `SEND` and `SUBSCRIBE`
- Server-side `SEND`, `SUBSCRIBE`, `UNSUBSCRIBE`, `DISCONNECT`, `RECEIPT`, and `ERROR`
- In-memory topic and queue destination routing
- Topic subscriptions with `*` and `**` wildcard matching
- Non-persistent queue subscriptions with round-robin delivery
- TCP server example
- Axum WebSocket server example
- Browser-based STOMP WebSocket debugger example
- E2E client test against ActiveMQ Artemis

## Workspace Layout

```text
crates/
  stompoxide-codec/
  stompoxide-client/
  stompoxide-server/
```

## Quick Start

Run the test suite:

```sh
cargo test
```

Check the workspace:

```sh
cargo check
```

Run the standalone TCP server example:

```sh
cargo run -p stompoxide-server --example tcp_server
```

The TCP server listens on `127.0.0.1:61613`.

## Client Example

```rust
use futures_util::StreamExt;
use stompoxide_client::{ClientConfig, SendRequest, StompClient, SubscribeRequest};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stream = TcpStream::connect("127.0.0.1:61613").await?;
    let (client, _handle) = StompClient::connect(stream, ClientConfig::default()).await?;
    let (sender, subscriber) = client.split();

    let mut subscription = subscriber
        .subscribe(SubscribeRequest::new("/queue/example"))
        .await?;
    sender
        .send(SendRequest::new("/queue/example", "hello from stompoxide"))
        .await?;

    if let Some(frame) = subscription.next().await {
        println!("received: {:?}", frame.body);
    }

    Ok(())
}
```

## Server Example

```rust
use stompoxide_server::StompServer;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    tracing_subscriber::fmt::init();

    let server = StompServer::new();
    server.listen_tcp("127.0.0.1:61613").await
}
```

## Destination Model

The server has two built-in destination families:

| Pattern | Kind | Behavior |
| --- | --- | --- |
| `/topic/**` | Topic | Broadcasts each message to all matching subscribers. |
| `/queue/**` | Queue | Delivers each message to one exact subscriber using round-robin. |

Topic subscriptions support wildcards:

```text
/topic/orders/*
/topic/orders/**
```

Queue subscriptions must use an exact destination:

```text
/queue/jobs
```

The server rejects unknown destinations and queue wildcard subscriptions with
an `ERROR` frame, then closes the connection. If a queue has no subscribers,
messages sent to that queue are dropped.

## WebSocket Debugger

The server crate includes an Axum WebSocket example and a browser debugger:

```sh
cargo run -p stompoxide-server --example axum_ws_server
```

Then open:

```text
crates/stompoxide-server/examples/stomp_client.html
```

The debugger can connect, subscribe, send messages, and optionally request
STOMP receipts.

The server crate also exposes `StompServer::connection_service()`, a Tower
service for framework adapters that can provide an `AsyncRead + AsyncWrite`
WebSocket stream.

## ActiveMQ Artemis E2E Test

The client test suite includes an E2E test that connects to a local ActiveMQ
Artemis broker on `127.0.0.1:61613`. If the broker is not running, the test is
skipped.

Start a local broker with:

```sh
docker run --detach --name activemq-artemis --network=host --rm apache/activemq-artemis:latest-alpine
```

The test uses the default Artemis credentials:

```text
login: artemis
passcode: artemis
```

## Current Scope

Stompoxide is currently a small protocol stack and testbed rather than a full
message broker. The server uses in-memory routing and does not persist messages,
track durable subscriptions, or implement transactional semantics. Queue
destinations are non-persistent and drop messages when no subscriber is present.

The client currently uses auto-ack subscriptions and exposes received messages
as an async stream.
