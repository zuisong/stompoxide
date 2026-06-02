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
- Negotiates STOMP `1.0`, `1.1`, and `1.2`; missing `accept-version` is treated
  as STOMP `1.0`.
- Requires `host` for STOMP `1.1` and `1.2` connections.
- Disables heartbeats for STOMP `1.0` and negotiates heartbeats for STOMP `1.1`
  and `1.2`.
- Supports `SEND`, `SUBSCRIBE`, `UNSUBSCRIBE`, `ACK`, `NACK`, `BEGIN`,
  `COMMIT`, `ABORT`, and `DISCONNECT`.
- Sends `RECEIPT` frames when a request includes a `receipt` header.
- Sends `ERROR` frames for unsupported commands or malformed requests.
- Optionally authenticates `login` and `passcode` headers with
  `StompServer::with_authenticator(...)`.
- Routes messages in memory by destination.
- Treats `/topic/**` as topic destinations.
- Treats `/queue/**` as non-persistent queue destinations.
- Rejects unknown destinations with an `ERROR` frame and closes the connection.
- Tracks client acknowledgements and redelivers messages on `NACK` or
  disconnect.

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

## Acknowledgements

The server supports `ack:auto`, `ack:client`, and `ack:client-individual`.
STOMP 1.0 does not support `client-individual`; such subscriptions are rejected.

ACK/NACK headers are validated according to the negotiated protocol version:

| Version | ACK | NACK |
| --- | --- | --- |
| STOMP 1.0 | `message-id` | Not supported |
| STOMP 1.1 | `message-id` and `subscription` | `message-id` and `subscription` |
| STOMP 1.2 | `id`, matching the `MESSAGE` frame's `ack` header | `id`, matching the `MESSAGE` frame's `ack` header |

For STOMP 1.2, each client-ack delivery gets a unique `ack` header. For STOMP
1.0 and 1.1, the server keeps an internal per-delivery id while preserving the
protocol-visible `message-id` ACK format. This prevents topic fan-out
deliveries from overwriting each other's pending acknowledgement state.

## Transactions

The server supports `BEGIN`, `COMMIT`, and `ABORT`. Transactional `SEND`,
`ACK`, and `NACK` frames are buffered under the transaction id. `COMMIT`
processes the buffered frames in order; `ABORT` discards them.

ACK/NACK validation still applies inside transactions. For example, a STOMP 1.1
transactional `ACK` must include both `message-id` and `subscription`.

## Authentication

`StompServer::with_authenticator(...)` installs a synchronous callback that
checks the `login` and `passcode` headers during connection setup:

```rust
use stompoxide_server::StompServer;

let server = StompServer::new().with_authenticator(|login, passcode| {
    login == "admin" && passcode == "secret"
});
```
