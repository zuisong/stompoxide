use std::borrow::Cow;

use bytes::{Buf, BytesMut};
use tokio_util::codec::{Decoder, Encoder};
use winnow::{
    ModalResult as PResult, ModalResult, Parser, Partial,
    ascii::{alpha1, line_ending, till_line_ending},
    combinator::{delimited, opt, repeat, separated_pair, terminated},
    error::ContextError,
    token::{literal, take, take_till, take_until},
};
#[cfg(test)]
mod tests;

pub const DEFAULT_MAX_BODY_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct StompFrame<'a> {
    pub command: Cow<'a, str>,
    pub headers: Vec<(String, String)>,
    pub body: Option<Cow<'a, [u8]>>,
}

impl<'a> StompFrame<'a> {
    pub fn into_owned(self) -> StompFrame<'static> {
        StompFrame {
            command: Cow::Owned(self.command.into_owned()),
            headers: self.headers,
            body: self.body.map(|b| Cow::Owned(b.into_owned())),
        }
    }

    pub fn get_header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StompVersion {
    V1_0,
    V1_1,
    #[default]
    V1_2,
}

impl StompFrame<'_> {
    pub fn serialize(&'_ self) -> Cow<'_, [u8]> {
        self.serialize_with_version(StompVersion::V1_2)
    }

    pub fn serialize_with_version(&'_ self, version: StompVersion) -> Cow<'_, [u8]> {
        let mut buf = Vec::new();
        let escape =
            !(self.command == "CONNECT" || self.command == "CONNECTED" || self.command == "STOMP");

        fn escaped(b: &u8, escape: bool, version: StompVersion) -> &[u8] {
            if escape {
                match version {
                    StompVersion::V1_0 => std::slice::from_ref(b),
                    StompVersion::V1_1 => match b {
                        b'\n' => b"\\n",
                        b':' => b"\\c",
                        b'\\' => b"\\\\",
                        bytes => std::slice::from_ref(bytes),
                    },
                    StompVersion::V1_2 => match b {
                        b'\r' => b"\\r",
                        b'\n' => b"\\n",
                        b':' => b"\\c",
                        b'\\' => b"\\\\",
                        bytes => std::slice::from_ref(bytes),
                    },
                }
            } else {
                std::slice::from_ref(b)
            }
        }

        buf.extend_from_slice(self.command.as_bytes());
        buf.push(b'\n');
        self.headers
            .iter()
            .filter(|(key, _)| key != "content-length")
            .for_each(|(key, val)| {
                for byte in key.as_bytes() {
                    buf.extend_from_slice(escaped(byte, escape, version));
                }
                buf.push(b':');
                for byte in val.as_bytes() {
                    buf.extend_from_slice(escaped(byte, escape, version));
                }
                buf.push(b'\n');
            });
        if let Some(body) = &self.body {
            buf.extend_from_slice(&get_content_length_header(body));
            buf.push(b'\n');
            buf.extend_from_slice(body);
        } else {
            buf.push(b'\n');
        }
        buf.push(b'\x00');
        Cow::Owned(buf)
    }
}

fn get_content_length(headers: &Vec<(String, String)>) -> Option<usize> {
    for (name, value) in headers {
        if name == "content-length" {
            return value.parse::<usize>().ok();
        }
    }
    None
}

fn map_empty_slice(s: &[u8]) -> Option<&[u8]> {
    Some(s).filter(|c| !c.is_empty())
}

pub fn parse_frame(input: &'_ [u8]) -> ModalResult<(&'_ [u8], StompFrame<'_>), ContextError> {
    parse_frame_with_version_and_max_body_size(input, StompVersion::V1_2, DEFAULT_MAX_BODY_SIZE)
}

pub fn parse_frame_with_version(
    input: &'_ [u8],
    version: StompVersion,
) -> ModalResult<(&'_ [u8], StompFrame<'_>), ContextError> {
    parse_frame_with_version_and_max_body_size(input, version, DEFAULT_MAX_BODY_SIZE)
}

pub fn parse_frame_with_version_and_max_body_size(
    input: &'_ [u8],
    version: StompVersion,
    max_body_size: usize,
) -> ModalResult<(&'_ [u8], StompFrame<'_>), ContextError> {
    let mut partial = Partial::new(input);
    let result = { parse_frame_stream(&mut partial, version, max_body_size)? };
    Ok((partial.into_inner(), result))
}

pub fn parse_frame_stream<'b>(
    input: &mut Partial<&'b [u8]>,
    version: StompVersion,
    max_body_size: usize,
) -> PResult<StompFrame<'b>> {
    let command_raw: &[u8] =
        delimited(repeat(0.., line_ending).map(|()| ()), alpha1, line_ending).parse_next(input)?;

    let command = String::from_utf8_lossy(command_raw);
    let escape = !(command == "CONNECT" || command == "CONNECTED" || command == "STOMP");

    let headers = terminated(
        repeat(0.., move |i: &mut _| parse_header(i, escape, version)),
        line_ending,
    )
    .parse_next(input)?;

    let body = match get_content_length(&headers) {
        None => take_until(0.., "\x00")
            .map(map_empty_slice)
            .parse_next(input)?,
        Some(length) => {
            if length > max_body_size {
                return Err(winnow::error::ErrMode::Cut(ContextError::new()));
            }
            take(length).map(Some).parse_next(input)?
        }
    };

    (literal("\0"), opt(line_ending.complete_err())).parse_next(input)?;

    Ok(StompFrame {
        command,
        headers,
        body: body.map(Cow::Borrowed),
    })
}

fn parse_header(
    input: &mut Partial<&[u8]>,
    escape: bool,
    version: StompVersion,
) -> PResult<(String, String)> {
    separated_pair(
        take_till(0.., [':', '\r', '\n'])
            .and_then(move |i: &mut &[u8]| parse_header_value(i, escape, version)),
        literal(":"),
        terminated(till_line_ending, line_ending)
            .and_then(move |i: &mut &[u8]| parse_header_value(i, escape, version)),
    )
    .parse_next(input)
}

fn parse_header_value(input: &mut &[u8], escape: bool, version: StompVersion) -> PResult<String> {
    let input_slice = *input;
    *input = b"";
    if escape
        && match version {
            StompVersion::V1_0 => false,
            StompVersion::V1_1 | StompVersion::V1_2 => true,
        }
    {
        unescape(input_slice, version)
    } else {
        String::from_utf8(input_slice.to_vec())
            .map_err(|_| winnow::error::ErrMode::Backtrack(ContextError::new()))
    }
}

fn unescape(input: &[u8], version: StompVersion) -> PResult<String> {
    let mut s = String::new();
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'\\' {
            if i + 1 >= input.len() {
                return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
            }
            match input[i + 1] {
                b'r' if version == StompVersion::V1_2 => s.push('\r'),
                b'n' => s.push('\n'),
                b'c' => s.push(':'),
                b'\\' => s.push('\\'),
                _ => {
                    return Err(winnow::error::ErrMode::Backtrack(ContextError::new()));
                }
            }
            i += 2;
        } else {
            let start = i;
            while i < input.len() && input[i] != b'\\' {
                i += 1;
            }
            let chunk = &input[start..i];
            let chunk_str = std::str::from_utf8(chunk)
                .map_err(|_| winnow::error::ErrMode::Backtrack(ContextError::new()))?;
            s.push_str(chunk_str);
        }
    }
    Ok(s)
}

fn get_content_length_header(body: &[u8]) -> Vec<u8> {
    format!("content-length:{}\n", body.len()).into_bytes()
}

pub struct StompCodec {
    pub version: StompVersion,
    pub max_body_size: usize,
}

impl Default for StompCodec {
    fn default() -> Self {
        Self {
            version: StompVersion::V1_2,
            max_body_size: DEFAULT_MAX_BODY_SIZE,
        }
    }
}

impl StompCodec {
    pub fn with_max_body_size(max_body_size: usize) -> Self {
        Self {
            version: StompVersion::V1_2,
            max_body_size,
        }
    }
}

impl Decoder for StompCodec {
    type Item = StompFrame<'static>;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Consume leading EOL heartbeats and yield a HEARTBEAT command frame
        let mut eol_count = 0;
        while eol_count < src.len() && (src[eol_count] == b'\n' || src[eol_count] == b'\r') {
            eol_count += 1;
        }
        if eol_count > 0 {
            src.advance(eol_count);
            return Ok(Some(StompFrame {
                command: Cow::Borrowed("HEARTBEAT"),
                headers: vec![],
                body: None,
            }));
        }

        let (consumed, frame_owned) = {
            let input = src.as_ref();
            match parse_frame_with_version_and_max_body_size(
                input,
                self.version,
                self.max_body_size,
            ) {
                Ok((remain, frame)) => {
                    let consumed = input.len() - remain.len();
                    (consumed, frame.into_owned())
                }
                Err(winnow::error::ErrMode::Incomplete(_)) => return Ok(None),
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("STOMP parse error: {:?}", e),
                    ));
                }
            }
        };
        src.advance(consumed);
        Ok(Some(frame_owned))
    }
}

impl Encoder<StompFrame<'_>> for StompCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: StompFrame<'_>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let serialized = item.serialize_with_version(self.version);
        dst.extend_from_slice(serialized.as_ref());
        Ok(())
    }
}
