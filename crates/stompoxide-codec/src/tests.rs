use assert_matches2::assert_matches;
use pretty_assertions::assert_eq;
use winnow::error::ErrMode::Incomplete;

use super::*;

#[test]
fn parse_and_serialize_connect() {
    let data = b"CONNECT
accept-version:1.2
host:datafeeds.here.co.uk
login:user
heart-beat:6,7
passcode:password\\c123\n\n\x00"
        .to_vec();
    let (_, frame) = parse_frame(&data).unwrap();
    assert_eq!(frame.command.as_ref(), "CONNECT");
    let headers_expect: Vec<(&[u8], &[u8])> = vec![
        (&b"accept-version"[..], &b"1.2"[..]),
        (b"host", b"datafeeds.here.co.uk"),
        (b"login", b"user"),
        (b"heart-beat", b"6,7"),
        // In STOMP 1.2, CONNECT and CONNECTED frames do not escape/unescape headers.
        (b"passcode", b"password\\c123"),
    ];
    let fh: Vec<_> = frame
        .headers
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
        .collect();

    assert_eq!(fh, headers_expect);
    assert_eq!(frame.body, None);
}

#[test]
fn parse_and_serialize_message() {
    let mut data = b"\nMESSAGE
destination:datafeeds.here.co.uk
message-id:12345
subscription:some-id
"
    .to_vec();
    let body = "this body contains \x00 nulls \n and \r\n newlines \x00 OK?";
    let rest = format!("content-length:{}\n\n{}\x00", body.len(), body);
    data.extend_from_slice(rest.as_bytes());
    let (_, frame) = parse_frame(&data).unwrap();
    assert_eq!(frame.command.as_bytes(), b"MESSAGE");
    let headers_expect: Vec<(&[u8], &[u8])> = vec![
        (&b"destination"[..], &b"datafeeds.here.co.uk"[..]),
        (b"message-id", b"12345"),
        (b"subscription", b"some-id"),
        (b"content-length", b"50"),
    ];
    let fh: Vec<_> = frame
        .headers
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
        .collect();
    assert_eq!(fh, headers_expect);
    assert_eq!(frame.body.as_ref().unwrap().as_ref(), (body.as_bytes()));
}

#[test]
fn parse_and_serialize_message_with_body_start_with_newline() {
    let mut data = b"MESSAGE
destination:datafeeds.here.co.uk
message-id:12345
subscription:some-id"
        .to_vec();
    let body = "\n\n\nthis body contains  nulls \n and \r\n newlines OK?";
    let rest = format!("\n\n{body}\x00\r\n");
    data.extend_from_slice(rest.as_bytes());
    let (_, frame) = parse_frame(&data).unwrap();
    assert_eq!(frame.command.as_bytes(), b"MESSAGE");
    let headers_expect: Vec<(&[u8], &[u8])> = vec![
        (&b"destination"[..], &b"datafeeds.here.co.uk"[..]),
        (b"message-id", b"12345"),
        (b"subscription", b"some-id"),
    ];
    let fh: Vec<_> = frame
        .headers
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
        .collect();
    assert_eq!(fh, headers_expect);
    assert_eq!(frame.body.unwrap(), (body.as_bytes()));
}

#[test]
fn parse_and_serialize_message_body_like_header() {
    let data = b"\nMESSAGE\r
destination:datafeeds.here.co.uk
message-id:12345
empty-header:
subscription:some-id\n\nsomething-like-header:1\x00\r\n"
        .to_vec();
    let (_, frame) = parse_frame(&data).unwrap();
    assert_eq!(frame.command.as_bytes(), b"MESSAGE");
    let headers_expect: Vec<(&[u8], &[u8])> = vec![
        (b"destination", b"datafeeds.here.co.uk"),
        (b"message-id", b"12345"),
        (b"empty-header", b""),
        (b"subscription", b"some-id"),
    ];
    let fh: Vec<_> = frame
        .headers
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
        .collect();
    assert_eq!(fh, headers_expect);
    assert_eq!(
        frame.body.as_ref().unwrap().as_ref(),
        ("something-like-header:1".as_bytes())
    );
}

#[test]
fn parse_a_incomplete_message() {
    assert_matches!(parse_frame(b"\nMESSAG".as_ref()), Err(Incomplete(_)));

    assert_matches!(parse_frame(b"\nMESSAGE\n\n".as_ref()), Err(Incomplete(_)));

    assert_matches!(
        parse_frame(b"\nMESSAG\n\n\0".as_ref()),
        Ok((
            _,
            StompFrame {
                ref command,
                headers,
                body: None,
            },
        ))
    );
    assert_eq!(headers, vec![]);
    assert_eq!(command, "MESSAG");

    assert_matches!(
        parse_frame(b"\nMESSAGE\r\ndestination:datafeeds.here.co.uk".as_ref()),
        Err(Incomplete(_))
    );

    assert_matches!(
        parse_frame(b"MESSAGE\r\ndestination:datafeeds.here.co.uk\n\n\0".as_ref()),
        Ok(([], StompFrame { .. }))
    );

    assert_matches!(
        parse_frame(b"\nMESSAGE\r\ndestination:datafeeds.here.co.uk\n\n".as_ref()),
        Err(Incomplete(_))
    );

    assert_matches!(
        parse_frame(b"\nMESSAGE\r\nheader:da\\ctafeeds.here.co.uk\n\n\0".as_ref()),
        Ok((b"", StompFrame { headers, .. }))
    );
    assert_eq!(
        headers,
        vec![("header".into(), "da:tafeeds.here.co.uk".into())]
    );

    assert_matches!(
        parse_frame(b"\nMESSAGE\r\ndestination:datafeeds.here.co.uk".as_ref()),
        Err(Incomplete(_))
    );

    assert_matches!(
        parse_frame(b"\nMESSAGE\r\ndestination:datafeeds.here.co.uk\n\n\0remain".as_ref()),
        Ok((b"remain", StompFrame { .. })),
        "stream with other after body end, should return remain text"
    );

    assert_matches!(
        parse_frame(b"\nMESSAGE\ncontent-length:10000\n\n\0remain".as_ref()),
        Err(Incomplete(_)),
        "content-length:10000, body size<10000, return incomplete"
    );

    assert_matches!(
        parse_frame(b"\nMESSAGE\ncontent-length:0\n\n\0remain".as_ref()),
        Ok((b"remain", StompFrame { body: Some(b), .. })),
        "empty body with content-length:0, body should be Some([])"
    );
    assert_eq!(b.len(), 0);
    assert_matches!(
        parse_frame(b"\nMESSAGE\n\n\0remain".as_ref()),
        Ok((b"remain", StompFrame { body: None, .. })),
        "empty body without content-length header, body should be None"
    );
}

#[test]
fn parse_and_serialize_message_header_value_with_colon() {
    let data = b"CONNECTED
server:ActiveMQ/6.0.0
heart-beat:0,0
session:ID:orbstack-45879-1702220142549-3:2
version:1.2

\0\n"
        .to_vec();
    let (_, frame) = parse_frame(&data).unwrap();
    assert_eq!(frame.command.as_bytes(), b"CONNECTED");
    let headers_expect: Vec<(&[u8], &[u8])> = vec![
        (b"server", b"ActiveMQ/6.0.0"),
        (b"heart-beat", b"0,0"),
        (b"session", b"ID:orbstack-45879-1702220142549-3:2"),
        (b"version", b"1.2"),
    ];
    let fh: Vec<_> = frame
        .headers
        .iter()
        .map(|(k, v)| (k.as_bytes(), v.as_bytes()))
        .collect();
    assert_eq!(fh, headers_expect);
}

#[test]
fn test_parser_header_unescape() {
    let h = parse_frame(
        b"MESSAGE
subscription:11
message-id:0.4.0
destination:now\\c Instant {\\n    tv_sec\\c 5740,\\n    tv_nsec\\c 164006416,\\n}
content-type:application/json
server:tokio-stomp/0.4.0

body\0"
            .as_ref(),
    );
    dbg!(&h);
    assert_matches!(
        h,
        Ok((
            b"",
            StompFrame {
                body: Some(ref b), ..
            },
        ))
    );
    assert_eq!(b.as_ref(), b"body")
}

#[test]
fn test_serialize1() {
    let f = StompFrame {
        command: "MESSAGE".into(),
        body: None,
        headers: vec![],
    };

    assert_eq!(
        f.serialize().as_ref(),
        b"MESSAGE

\0"
    );
}
#[test]
fn test_serialize2() {
    let f = StompFrame {
        command: "MESSAGE".into(),
        body: Some(b"body".as_slice().into()),
        headers: vec![],
    };

    assert_eq!(
        f.serialize().as_ref(),
        b"MESSAGE
content-length:4

body\0"
    );
}
#[test]
fn test_serialize3() {
    let f = StompFrame {
        command: "MESSAGE".into(),
        body: Some(b"body".as_slice().into()),
        headers: vec![("name\r\n:\\end".to_string(), "value\r\n:".to_string())],
    };

    assert_eq!(
        f.serialize().as_ref(),
        b"MESSAGE
name\\r\\n\\c\\\\end:value\\r\\n\\c
content-length:4

body\0"
    );
}

#[test]
fn test_long_body() {
    let body = "b\ndy".repeat(1000);
    let f = StompFrame {
        command: "MESSAGE".into(),
        body: Some(body.as_bytes().into()),
        headers: vec![("name\r\n:\\end".to_string(), "value\r\n:".to_string())],
    };

    assert_eq!(
        f.serialize(),
        format!(
            "MESSAGE
name\\r\\n\\c\\\\end:value\\r\\n\\c
content-length:{}

{}\0",
            body.len(),
            body
        )
        .as_bytes()
    );
}

#[test]
fn test_invalid_escape_sequence_fails() {
    // \t is an undefined escape sequence, which must fail
    let data = b"MESSAGE
destination:test
invalid-header:value\\twith\\tinvalid\\tescape

\0";
    let res = parse_frame(data);
    assert!(
        res.is_err(),
        "Undefined escape sequences must cause a parsing error"
    );
}

#[test]
fn test_multiple_leading_eol_heartbeats() {
    // Leading and trailing heartbeats (multiple EOLs) between frames
    let data = b"\n\r\n\nMESSAGE\ndestination:test\n\n\0\n\r\n";
    let (remain, frame) = parse_frame(data).unwrap();
    assert_eq!(frame.command, "MESSAGE");
    assert_eq!(remain, b"\r\n");
}

#[test]
fn test_connect_frame_does_not_escape() {
    let frame = StompFrame {
        command: "CONNECT".into(),
        headers: vec![("passcode".to_string(), "pass:word\\c123".to_string())],
        body: None,
    };
    let serialized = frame.serialize();
    assert_eq!(
        serialized.as_ref(),
        b"CONNECT\npasscode:pass:word\\c123\n\n\0"
    );

    let (_, parsed) = parse_frame(&serialized).unwrap();
    assert_eq!(parsed.headers[0].1, "pass:word\\c123");
}

#[test]
fn test_codec_decode_encode() {
    let data = b"MESSAGE\ndestination:test\ncontent-length:5\n\nhello\0";
    let mut buf = BytesMut::from(&data[..]);
    let mut codec = StompCodec::default();

    // Test decoding
    let frame = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(frame.command, "MESSAGE");
    assert_eq!(frame.headers[0].1, "test");
    assert_eq!(frame.body.as_ref().unwrap().as_ref(), b"hello");
    assert!(buf.is_empty());

    // Test encoding
    let mut out_buf = BytesMut::new();
    codec.encode(frame, &mut out_buf).unwrap();
    assert_eq!(out_buf.as_ref(), data);
}

#[test]
fn test_codec_incomplete_decode() {
    let data = b"MESSAGE\ndestination:test\n\nhel";
    let mut buf = BytesMut::from(&data[..]);
    let mut codec = StompCodec::default();

    let res = codec.decode(&mut buf).unwrap();
    assert!(res.is_none(), "Should return None on incomplete data");
    assert_eq!(buf.len(), data.len(), "Buffer should not be advanced");
}

#[test]
fn test_user_scenario_content_length() {
    let data = b"SEND
destination:/topic/aaaa
content-length:4

aaaa\0";
    let (remain, frame) = parse_frame(data).unwrap();
    assert_eq!(frame.command, "SEND");
    assert_eq!(frame.body.as_ref().unwrap().as_ref(), b"aaaa");
    assert_eq!(remain, b"");
}

#[test]
fn test_heartbeat_decoding() {
    let mut buf = BytesMut::from(&b"\n\nMESSAGE\ndestination:test\n\n\0"[..]);
    let mut codec = StompCodec::default();

    // First decode yields HEARTBEAT because it consumes both leading newlines
    let hb1 = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(hb1.command, "HEARTBEAT");

    // Second decode yields the actual MESSAGE frame
    let msg = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(msg.command, "MESSAGE");
}

#[test]
fn test_get_header() {
    let frame = StompFrame {
        command: "MESSAGE".into(),
        headers: vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
            ("key1".to_string(), "val3".to_string()), // duplicate key
        ],
        body: None,
    };
    assert_eq!(frame.get_header("key1"), Some("val1"));
    assert_eq!(frame.get_header("key2"), Some("val2"));
    assert_eq!(frame.get_header("key3"), None);
}

#[test]
fn test_stomp_frame_does_not_escape() {
    let frame = StompFrame {
        command: "STOMP".into(),
        headers: vec![("passcode".to_string(), "pass:word\\c123".to_string())],
        body: None,
    };
    let serialized = frame.serialize();
    assert_eq!(
        serialized.as_ref(),
        b"STOMP\npasscode:pass:word\\c123\n\n\0"
    );

    let (_, parsed) = parse_frame(&serialized).unwrap();
    assert_eq!(parsed.headers[0].1, "pass:word\\c123");
}
