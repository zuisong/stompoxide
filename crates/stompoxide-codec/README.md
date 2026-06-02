# stompoxide-codec

`stompoxide-codec` provides STOMP frame parsing, serialization, and
`tokio-util` codec integration.

## Main Types

- `StompFrame`: an owned or borrowed STOMP frame representation
- `StompCodec`: a `tokio_util::codec::Decoder` and `Encoder`
- `StompVersion`: protocol version selector for STOMP 1.0, 1.1, and 1.2
- `parse_frame`: parse one frame from a byte slice

## Example

```rust
use stompoxide_codec::parse_frame;

let input = b"SEND\ndestination:/queue/demo\n\nhello\0";
let (_remaining, frame) = parse_frame(input)?;

assert_eq!(frame.command, "SEND");
assert_eq!(frame.body.as_deref(), Some(&b"hello"[..]));
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Notes

- `CONNECT` and `CONNECTED` frames follow the STOMP escaping rules for those
  commands.
- Non-connection frame header escaping depends on `StompVersion`:
  - STOMP 1.0 does not escape header values.
  - STOMP 1.1 escapes LF, colon, and backslash.
  - STOMP 1.2 escapes CR, LF, colon, and backslash.
- Invalid escape sequences fail parsing. In particular, `\r` is only valid in
  STOMP 1.2.
- Leading end-of-line bytes are decoded as `HEARTBEAT` frames.
- Frames with a body are serialized with a `content-length` header.
- `StompCodec::default()` starts in STOMP 1.2 mode. Clients and servers update
  the codec version after protocol negotiation.
