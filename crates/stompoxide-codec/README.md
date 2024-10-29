# stompoxide-codec

`stompoxide-codec` provides STOMP frame parsing, serialization, and
`tokio-util` codec integration.

## Main Types

- `StompFrame`: an owned or borrowed STOMP frame representation
- `StompCodec`: a `tokio_util::codec::Decoder` and `Encoder`
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
- Leading end-of-line bytes are decoded as `HEARTBEAT` frames.
- Frames with a body are serialized with a `content-length` header.

