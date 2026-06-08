use std::{io, sync::Arc};

use bytes::Bytes;
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, join};
use tokio_util::io::{CopyToBytes, SinkWriter, StreamReader};

/// Adapt a WebSocket stream of messages to/from `Bytes` as an `AsyncRead` + `AsyncWrite` type.
#[doc(hidden)]
pub fn websocket_io<S, Msg, E, FRead, FWrite>(
    websocket: S,
    read_fn: FRead,
    write_fn: FWrite,
) -> impl AsyncRead + AsyncWrite + Send + 'static
where
    S: Stream<Item = Result<Msg, E>> + Sink<Msg, Error = E> + Send + 'static,
    Msg: Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
    FRead: Fn(Msg) -> Option<Bytes> + Send + Sync + 'static,
    FWrite: Fn(Bytes) -> Msg + Send + Sync + 'static,
{
    let (write_half, read_half) = websocket.split();

    let read_fn = Arc::new(read_fn);
    let reader = StreamReader::new(read_half.filter_map(move |message| {
        let read_fn = read_fn.clone();
        async move {
            match message {
                Ok(msg) => read_fn(msg).map(Ok),
                Err(error) => Some(Err(io::Error::new(io::ErrorKind::ConnectionReset, error))),
            }
        }
    }));

    let write_fn = Arc::new(write_fn);
    let writer = SinkWriter::new(CopyToBytes::new(
        write_half
            .sink_map_err(|error| io::Error::new(io::ErrorKind::ConnectionReset, error))
            .with(move |bytes: Bytes| {
                let write_fn = write_fn.clone();
                async move { Ok::<Msg, io::Error>(write_fn(bytes)) }
            }),
    ));

    join(reader, writer)
}
