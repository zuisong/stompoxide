use std::borrow::Cow;

use winnow::{
    ModalResult, Parser, Partial,
    ascii::{alpha1, escaped, line_ending, till_line_ending},
    combinator::{alt, delimited, opt, repeat, separated_pair, terminated},
    error::ContextError,
    token::{literal, take, take_till, take_until, take_while},
};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct StompFrame<'a> {
    command: Cow<'a, str>,
    headers: Vec<(String, String)>,
    body: Option<Cow<'a, [u8]>>,
}

impl StompFrame<'_> {
    pub fn serialize(&'_ self) -> Cow<'_, [u8]> {
        let mut buffer = Vec::new();
        fn escaped(b: &u8) -> &[u8] {
            let escaped: &[u8] = match b {
                b'\r' => b"\\r",
                b'\n' => b"\\n",
                b':' => b"\\c",
                b'\\' => b"\\\\",
                bytes => std::slice::from_ref(bytes),
            };
            escaped
        }
        buffer.extend_from_slice(self.command.as_bytes());
        buffer.push(b'\n');
        self.headers.iter().for_each(|(key, val)| {
            for byte in key.as_bytes() {
                buffer.extend_from_slice(escaped(byte));
            }
            buffer.push(b':');
            for byte in val.as_bytes() {
                buffer.extend_from_slice(escaped(byte));
            }
            buffer.push(b'\n');
        });
        if let Some(body) = &self.body {
            buffer.extend_from_slice(&get_content_length_header(body));
            buffer.push(b'\n');
            buffer.extend_from_slice(body);
        } else {
            buffer.push(b'\n');
        }
        buffer.push(b'\x00');
        Cow::Owned(buffer)
    }
}

fn get_content_length(headers: &Vec<(String, String)>) -> Option<usize> {
    for (name, value) in headers {
        if *name == "content-length" {
            return value.parse::<usize>().ok();
        }
    }
    None
}

fn map_empty_slice(s: &[u8]) -> Option<&[u8]> {
    Some(s).filter(|c| !c.is_empty())
}

pub fn parse_frame(input: &'_ [u8]) -> ModalResult<(&'_ [u8], StompFrame<'_>), ContextError> {
    let mut partial = Partial::new(input);
    let result = { parse_frame_stream(&mut partial)? };
    Ok((partial.into_inner(), result))
}

pub fn parse_frame_stream<'b>(input: &mut Partial<&'b [u8]>) -> ModalResult<StompFrame<'b>> {
    // dbg!(&String::from_utf8_lossy(input));

    let (command, headers) = (
        delimited(
            opt(line_ending),
            alpha1.map(String::from_utf8_lossy),
            line_ending,
        ), // command
        terminated(
            repeat(0.., parse_header), // header
            line_ending,
        ),
    )
        .parse_next(input)?;

    let body = match get_content_length(&headers) {
        None => take_until(0.., "\x00")
            .map(map_empty_slice)
            .parse_next(input)?,
        Some(length) => take(length).map(Some).parse_next(input)?,
    };

    (literal("\0"), opt(line_ending.complete_err())).parse_next(input)?;

    Ok(StompFrame {
        command,
        headers,
        body: body.map(Cow::Borrowed),
    })
}

fn parse_header(input: &mut Partial<&[u8]>) -> ModalResult<(String, String)> {
    separated_pair(
        take_till(0.., [':', '\r', '\n']),
        literal(":"),
        terminated(till_line_ending, line_ending),
    )
    .parse_next(input)
    .and_then(|(k, v)| Ok((unescape(k)?, unescape(v)?)))
}

fn unescape(input: &[u8]) -> ModalResult<String, ContextError> {
    let mut f = escaped(
        take_while(1, |c| c != b'\\'),
        '\\',
        alt((
            literal("\\").value(b'\\'),
            literal("r").value(b'\r'),
            literal("n").value(b'\n'),
            literal("c").value(b':'),
        )),
    )
    .try_map(String::from_utf8);

    f.parse_peek(input).map(|it| it.1)
}

fn get_content_length_header(body: &[u8]) -> Vec<u8> {
    format!("content-length:{}\n", body.len()).into_bytes()
}
