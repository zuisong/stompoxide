# stompoxide-server

`stompoxide-server` is a lightweight async STOMP server with in-memory pub/sub
routing.

It is useful for tests, examples, local development, and simple embedded
messaging scenarios.

## TCP Example

```rust
use stompoxide_server::StompServer;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    tracing_subscriber::fmt::init();

    let server = StompServer::new();
    server.listen_tcp("127.0.0.1:61613").await
}
```

Run the included example:

```sh
cargo run -p stompoxide-server --example tcp_server
```

## WebSocket Example

Run the Axum WebSocket example:

```sh
cargo run -p stompoxide-server --example axum_ws_server
```

Then open the browser debugger:

```text
crates/stompoxide-server/examples/stomp_client.html
```

## Tower Integration

`StompServer::connection_service()` returns a Tower service that accepts any
stream implementing `AsyncRead + AsyncWrite + Send + Unpin + 'static`.

Framework adapters can convert WebSocket connections into such a stream and
then pass them to the service:

```rust
use tower::ServiceExt;

let server = StompServer::new();
let service = server.connection_service();

service.oneshot(stream).await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

This keeps STOMP protocol handling independent from Axum, Hyper, or any other
specific web framework.

## Behavior

- Accepts `CONNECT` and `STOMP` as initial connection commands.
- Accepts STOMP `1.2` connections.
- Sends and checks STOMP `1.2` heartbeats.
- Supports `SEND`, `SUBSCRIBE`, `UNSUBSCRIBE`, and `DISCONNECT`.
- Sends `RECEIPT` frames when a request includes a `receipt` header.
- Sends `ERROR` frames for unsupported commands or malformed requests.
- Routes messages in memory by destination.
- Treats `/topic/**` as topic destinations.
- Treats `/queue/**` as non-persistent queue destinations.
- Rejects unknown destinations with an `ERROR` frame and closes the connection.

## Destination Matching

The server has two built-in destination families:

| Pattern | Kind | Behavior |
| --- | --- | --- |
| `/topic/**` | Topic | Broadcasts each message to all matching subscribers. |
| `/queue/**` | Queue | Delivers each message to one exact subscriber using round-robin. |

Topic subscriptions support simple wildcard matching:

- `*` matches one path segment.
- `**` matches the rest of a path.

For example, a subscription to `/topic/orders/*` matches
`/topic/orders/created`, while `/topic/orders/**` also matches deeper paths.

Queue subscriptions must use exact destinations:

```text
/queue/jobs
```

Wildcard queue subscriptions such as `/queue/*` and `/queue/**` are rejected.
If a queue has no subscribers, messages sent to that queue are dropped. Queue
messages are not persisted.
