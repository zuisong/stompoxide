use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures_core::Stream;
use futures_util::{SinkExt, StreamExt};
use stompoxide_codec::{StompCodec, StompFrame, StompVersion};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub enum StompError {
    Io(std::io::Error),
    Protocol(String),
    Disconnected,
    ReceiptTimeout,
}

impl fmt::Display for StompError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Protocol(s) => write!(f, "Protocol error: {}", s),
            Self::Disconnected => write!(f, "Disconnected from server"),
            Self::ReceiptTimeout => write!(f, "Receipt timeout"),
        }
    }
}

impl std::error::Error for StompError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Protocol(_) => None,
            Self::Disconnected => None,
            Self::ReceiptTimeout => None,
        }
    }
}

impl From<std::io::Error> for StompError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

/// Configuration for the STOMP Client.
///
/// # Examples
/// ```
/// use stompoxide_client::ClientConfig;
///
/// let config = ClientConfig {
///     host: "localhost".to_string(),
///     login: None,
///     passcode: None,
///     heartbeat_cx: 0,
///     heartbeat_cy: 0,
///     accept_versions: vec!["1.2".to_string()],
/// };
/// assert_eq!(config.host, "localhost");
/// ```
#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub host: String,
    pub login: Option<String>,
    pub passcode: Option<String>,
    pub heartbeat_cx: u32,
    pub heartbeat_cy: u32,
    pub accept_versions: Vec<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            login: None,
            passcode: None,
            heartbeat_cx: 0,
            heartbeat_cy: 0,
            accept_versions: vec!["1.0".to_string(), "1.1".to_string(), "1.2".to_string()],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AckMode {
    Auto,
    Client,
    ClientIndividual,
}

impl AckMode {
    fn as_header_value(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Client => "client",
            Self::ClientIndividual => "client-individual",
        }
    }
}

#[derive(Clone, Debug)]
pub struct SendRequest {
    destination: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl SendRequest {
    pub fn new(destination: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            destination: destination.into(),
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers = headers;
        self
    }

    pub fn receipt(mut self, receipt_id: impl Into<String>) -> Self {
        upsert_header(&mut self.headers, "receipt", receipt_id.into());
        self
    }

    pub fn transaction(mut self, transaction_id: impl Into<String>) -> Self {
        upsert_header(&mut self.headers, "transaction", transaction_id.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct SubscribeRequest {
    destination: String,
    headers: Vec<(String, String)>,
    id: Option<String>,
}

impl SubscribeRequest {
    pub fn new(destination: impl Into<String>) -> Self {
        Self {
            destination: destination.into(),
            headers: Vec::new(),
            id: None,
        }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    pub fn headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers = headers;
        self
    }

    pub fn receipt(mut self, receipt_id: impl Into<String>) -> Self {
        upsert_header(&mut self.headers, "receipt", receipt_id.into());
        self
    }

    pub fn ack(mut self, ack: AckMode) -> Self {
        upsert_header(&mut self.headers, "ack", ack.as_header_value().to_string());
        self
    }
}

fn upsert_header(headers: &mut Vec<(String, String)>, name: &str, value: String) {
    if let Some((_, existing_value)) = headers.iter_mut().find(|(key, _)| key == name) {
        *existing_value = value;
    } else {
        headers.push((name.to_string(), value));
    }
}

enum ClientCmd {
    Send {
        request: SendRequest,
        resp: oneshot::Sender<Result<(), StompError>>,
    },
    Subscribe {
        request: SubscribeRequest,
        sender: mpsc::UnboundedSender<StompFrame<'static>>,
        resp: oneshot::Sender<Result<(), StompError>>,
    },
    Unsubscribe {
        id: String,
        resp: oneshot::Sender<Result<(), StompError>>,
    },
    Ack {
        request: AckRequest,
        resp: oneshot::Sender<Result<(), StompError>>,
    },
    Nack {
        request: AckRequest,
        resp: oneshot::Sender<Result<(), StompError>>,
    },
    Begin {
        transaction_id: String,
        resp: oneshot::Sender<Result<(), StompError>>,
    },
    Commit {
        transaction_id: String,
        resp: oneshot::Sender<Result<(), StompError>>,
    },
    Abort {
        transaction_id: String,
        resp: oneshot::Sender<Result<(), StompError>>,
    },
    Disconnect {
        resp: oneshot::Sender<Result<(), StompError>>,
    },
}

enum AckCommand {
    Ack,
    Nack,
}

#[derive(Clone, Debug)]
pub struct AckRequest {
    id: String,
    subscription: Option<String>,
    transaction: Option<String>,
}

impl AckRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            subscription: None,
            transaction: None,
        }
    }

    pub fn subscription(mut self, subscription: impl Into<String>) -> Self {
        self.subscription = Some(subscription.into());
        self
    }

    pub fn transaction(mut self, transaction: impl Into<String>) -> Self {
        self.transaction = Some(transaction.into());
        self
    }
}

fn ack_headers(
    request: AckRequest,
    negotiated_version: StompVersion,
) -> Result<Vec<(String, String)>, StompError> {
    let mut headers = Vec::new();
    match negotiated_version {
        StompVersion::V1_0 => {
            headers.push(("message-id".to_string(), request.id));
        }
        StompVersion::V1_1 => {
            let subscription = request.subscription.ok_or_else(|| {
                StompError::Protocol("STOMP 1.1 ACK requires a subscription header".to_string())
            })?;
            headers.push(("message-id".to_string(), request.id));
            headers.push(("subscription".to_string(), subscription));
        }
        StompVersion::V1_2 => {
            headers.push(("id".to_string(), request.id));
        }
    }
    if let Some(transaction) = request.transaction {
        headers.push(("transaction".to_string(), transaction));
    }
    Ok(headers)
}

fn nack_headers(
    request: AckRequest,
    negotiated_version: StompVersion,
) -> Result<Vec<(String, String)>, StompError> {
    if negotiated_version == StompVersion::V1_0 {
        return Err(StompError::Protocol(
            "STOMP 1.0 does not support NACK".to_string(),
        ));
    }

    let mut headers = Vec::new();
    match negotiated_version {
        StompVersion::V1_1 => {
            let subscription = request.subscription.ok_or_else(|| {
                StompError::Protocol("STOMP 1.1 NACK requires a subscription header".to_string())
            })?;
            headers.push(("message-id".to_string(), request.id));
            headers.push(("subscription".to_string(), subscription));
        }
        StompVersion::V1_2 => {
            headers.push(("id".to_string(), request.id));
        }
        StompVersion::V1_0 => unreachable!(),
    }
    if let Some(transaction) = request.transaction {
        headers.push(("transaction".to_string(), transaction));
    }
    Ok(headers)
}

#[derive(Clone)]
pub struct StompClient {
    cmd_tx: mpsc::Sender<ClientCmd>,
    next_id: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct StompSender {
    cmd_tx: mpsc::Sender<ClientCmd>,
}

#[derive(Clone)]
pub struct StompSubscriber {
    cmd_tx: mpsc::Sender<ClientCmd>,
    next_id: Arc<AtomicU64>,
}

impl StompClient {
    /// Connects to a STOMP server over any TCP/generic Stream.
    pub async fn connect<S>(
        stream: S,
        config: ClientConfig,
    ) -> Result<(Self, tokio::task::JoinHandle<Result<(), StompError>>), StompError>
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (reader, writer) = tokio::io::split(stream);
        let mut framed_read = FramedRead::new(reader, StompCodec::default());
        let mut framed_write = FramedWrite::new(writer, StompCodec::default());

        // Send CONNECT frame
        let is_1_0_only = config.accept_versions.len() == 1 && config.accept_versions[0] == "1.0";
        let mut headers = Vec::new();
        if !is_1_0_only {
            headers.push((
                "accept-version".to_string(),
                config.accept_versions.join(","),
            ));
            headers.push(("host".to_string(), config.host.clone()));
            headers.push((
                "heart-beat".to_string(),
                format!("{},{}", config.heartbeat_cx, config.heartbeat_cy),
            ));
        }
        if let Some(ref login) = config.login {
            headers.push(("login".to_string(), login.clone()));
        }
        if let Some(ref passcode) = config.passcode {
            headers.push(("passcode".to_string(), passcode.clone()));
        }

        let connect_frame = StompFrame {
            command: Cow::Borrowed("CONNECT"),
            headers,
            body: None,
        };

        framed_write.send(connect_frame).await?;

        // Wait for CONNECTED frame
        let connected_frame = loop {
            match framed_read.next().await {
                Some(Ok(frame)) => {
                    if frame.command == "HEARTBEAT" {
                        continue;
                    }
                    if frame.command != "CONNECTED" {
                        return Err(StompError::Protocol(format!(
                            "Expected CONNECTED frame, got {}",
                            frame.command
                        )));
                    }
                    break frame;
                }
                Some(Err(e)) => return Err(StompError::Io(e)),
                None => return Err(StompError::Disconnected),
            }
        };

        // Update version in codec
        let version_str = connected_frame.get_header("version").unwrap_or("1.0");
        let negotiated_version = match version_str {
            "1.0" => StompVersion::V1_0,
            "1.1" => StompVersion::V1_1,
            "1.2" => StompVersion::V1_2,
            _ => StompVersion::V1_2,
        };

        framed_read.decoder_mut().version = negotiated_version;
        framed_write.encoder_mut().version = negotiated_version;

        // Negotiate heartbeat
        let mut hb_cx = 0;
        let mut hb_cy = 0;
        if let Some(val) = connected_frame.get_header("heart-beat") {
            let parts: Vec<&str> = val.split(',').collect();
            if parts.len() == 2 {
                hb_cx = parts[0].parse().unwrap_or(0);
                hb_cy = parts[1].parse().unwrap_or(0);
            }
        }

        // cx, cy negotiated intervals:
        // Outgoing heartbeat: client wants to send every config.heartbeat_cx, server expects every hb_cy.
        let mut outgoing_interval = if config.heartbeat_cx > 0 && hb_cy > 0 {
            std::cmp::max(config.heartbeat_cx, hb_cy)
        } else {
            0
        };

        // Incoming heartbeat: client expects every config.heartbeat_cy, server wants to send every hb_cx.
        let mut incoming_interval = if config.heartbeat_cy > 0 && hb_cx > 0 {
            std::cmp::max(config.heartbeat_cy, hb_cx)
        } else {
            0
        };

        if negotiated_version == StompVersion::V1_0 {
            outgoing_interval = 0;
            incoming_interval = 0;
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        let client = Self {
            cmd_tx,
            next_id: Arc::new(AtomicU64::new(1)),
        };

        // Spawn background connection task
        let handle = tokio::spawn(async move {
            run_connection_loop(
                framed_read,
                framed_write,
                cmd_rx,
                outgoing_interval,
                incoming_interval,
                negotiated_version,
            )
            .await
        });

        Ok((client, handle))
    }

    pub fn sender(&self) -> StompSender {
        StompSender {
            cmd_tx: self.cmd_tx.clone(),
        }
    }

    pub fn subscriber(&self) -> StompSubscriber {
        StompSubscriber {
            cmd_tx: self.cmd_tx.clone(),
            next_id: self.next_id.clone(),
        }
    }

    pub fn split(self) -> (StompSender, StompSubscriber) {
        let subscriber = StompSubscriber {
            cmd_tx: self.cmd_tx.clone(),
            next_id: self.next_id,
        };
        let sender = StompSender {
            cmd_tx: self.cmd_tx,
        };
        (sender, subscriber)
    }

    pub async fn send(&self, request: SendRequest) -> Result<(), StompError> {
        self.sender().send(request).await
    }

    pub async fn subscribe(&self, request: SubscribeRequest) -> Result<Subscription, StompError> {
        self.subscriber().subscribe(request).await
    }

    pub async fn ack(&self, request: AckRequest) -> Result<(), StompError> {
        self.sender().ack(request).await
    }

    pub async fn nack(&self, request: AckRequest) -> Result<(), StompError> {
        self.sender().nack(request).await
    }

    pub async fn begin(&self, transaction_id: impl Into<String>) -> Result<(), StompError> {
        self.sender().begin(transaction_id).await
    }

    pub async fn commit(&self, transaction_id: impl Into<String>) -> Result<(), StompError> {
        self.sender().commit(transaction_id).await
    }

    pub async fn abort(&self, transaction_id: impl Into<String>) -> Result<(), StompError> {
        self.sender().abort(transaction_id).await
    }

    pub async fn disconnect(&self) -> Result<(), StompError> {
        self.sender().disconnect().await
    }
}

impl StompSender {
    pub async fn send(&self, request: SendRequest) -> Result<(), StompError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCmd::Send {
                request,
                resp: resp_tx,
            })
            .await
            .map_err(|_| StompError::Disconnected)?;

        resp_rx.await.map_err(|_| StompError::Disconnected)?
    }

    pub async fn ack(&self, request: AckRequest) -> Result<(), StompError> {
        self.send_ack(AckCommand::Ack, request).await
    }

    pub async fn nack(&self, request: AckRequest) -> Result<(), StompError> {
        self.send_ack(AckCommand::Nack, request).await
    }

    async fn send_ack(&self, command: AckCommand, request: AckRequest) -> Result<(), StompError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = match command {
            AckCommand::Ack => ClientCmd::Ack {
                request,
                resp: resp_tx,
            },
            AckCommand::Nack => ClientCmd::Nack {
                request,
                resp: resp_tx,
            },
        };
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| StompError::Disconnected)?;

        resp_rx.await.map_err(|_| StompError::Disconnected)?
    }

    pub async fn begin(&self, transaction_id: impl Into<String>) -> Result<(), StompError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCmd::Begin {
                transaction_id: transaction_id.into(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| StompError::Disconnected)?;

        resp_rx.await.map_err(|_| StompError::Disconnected)?
    }

    pub async fn commit(&self, transaction_id: impl Into<String>) -> Result<(), StompError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCmd::Commit {
                transaction_id: transaction_id.into(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| StompError::Disconnected)?;

        resp_rx.await.map_err(|_| StompError::Disconnected)?
    }

    pub async fn abort(&self, transaction_id: impl Into<String>) -> Result<(), StompError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCmd::Abort {
                transaction_id: transaction_id.into(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| StompError::Disconnected)?;

        resp_rx.await.map_err(|_| StompError::Disconnected)?
    }

    pub async fn disconnect(&self) -> Result<(), StompError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCmd::Disconnect { resp: resp_tx })
            .await
            .map_err(|_| StompError::Disconnected)?;

        resp_rx.await.map_err(|_| StompError::Disconnected)?
    }
}

impl StompSubscriber {
    pub async fn subscribe(
        &self,
        mut request: SubscribeRequest,
    ) -> Result<Subscription, StompError> {
        let sub_id = request
            .id
            .take()
            .unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::SeqCst).to_string());
        request.id = Some(sub_id.clone());
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();

        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCmd::Subscribe {
                request,
                sender: msg_tx,
                resp: resp_tx,
            })
            .await
            .map_err(|_| StompError::Disconnected)?;

        resp_rx.await.map_err(|_| StompError::Disconnected)??;

        Ok(Subscription {
            id: sub_id,
            cmd_tx: self.cmd_tx.clone(),
            stream: tokio_stream::wrappers::UnboundedReceiverStream::new(msg_rx),
            auto_unsubscribe: true,
        })
    }
}

pub struct Subscription {
    id: String,
    cmd_tx: mpsc::Sender<ClientCmd>,
    stream: tokio_stream::wrappers::UnboundedReceiverStream<StompFrame<'static>>,
    auto_unsubscribe: bool,
}

impl Subscription {
    pub async fn unsubscribe(mut self) -> Result<(), StompError> {
        self.auto_unsubscribe = false;
        let (resp_tx, resp_rx) = oneshot::channel();
        self.cmd_tx
            .send(ClientCmd::Unsubscribe {
                id: self.id.clone(),
                resp: resp_tx,
            })
            .await
            .map_err(|_| StompError::Disconnected)?;

        resp_rx.await.map_err(|_| StompError::Disconnected)?
    }
}

impl Stream for Subscription {
    type Item = StompFrame<'static>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        if !self.auto_unsubscribe {
            return;
        }
        let id = self.id.clone();
        let (resp_tx, _resp_rx) = oneshot::channel();
        let cmd = ClientCmd::Unsubscribe { id, resp: resp_tx };
        match self.cmd_tx.try_send(cmd) {
            Ok(()) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(cmd)) => {
                let cmd_tx = self.cmd_tx.clone();
                tokio::spawn(async move {
                    let _ = cmd_tx.send(cmd).await;
                });
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
        }
    }
}

async fn run_connection_loop<R, W>(
    mut reader: FramedRead<R, StompCodec>,
    mut writer: FramedWrite<W, StompCodec>,
    mut cmd_rx: mpsc::Receiver<ClientCmd>,
    outgoing_hb: u32,
    incoming_hb: u32,
    negotiated_version: StompVersion,
) -> Result<(), StompError>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let mut subscriptions = HashMap::new();
    let mut outgoing_timer = if outgoing_hb > 0 {
        Some(tokio::time::interval(Duration::from_millis(
            outgoing_hb as u64,
        )))
    } else {
        None
    };

    // Incoming heartbeat check (with 1.5x tolerance as recommended in STOMP spec)
    let incoming_timeout = if incoming_hb > 0 {
        Some(Duration::from_millis((incoming_hb as f64 * 1.5) as u64))
    } else {
        None
    };

    let mut last_received = std::time::Instant::now();
    let mut incoming_checker = if let Some(timeout) = incoming_timeout {
        let check_duration = std::cmp::min(timeout / 2, Duration::from_secs(1));
        Some(tokio::time::interval(check_duration))
    } else {
        None
    };

    loop {
        let read_future = reader.next();
        let cmd_future = cmd_rx.recv();

        tokio::select! {
            // Outgoing heartbeat trigger
            Some(_) = async {
                if let Some(ref mut timer) = outgoing_timer {
                    Some(timer.tick().await)
                } else {
                    None
                }
            } => {
                // Send raw EOL heartbeat directly to the writer and flush
                if let Err(e) = writer.get_mut().write_all(b"\n").await {
                    return Err(StompError::Io(e));
                }
                if let Err(e) = writer.get_mut().flush().await {
                    return Err(StompError::Io(e));
                }
            }

            // Incoming heartbeat checker
            Some(_) = async {
                if let Some(ref mut checker) = incoming_checker {
                    Some(checker.tick().await)
                } else {
                    None
                }
            } => {
                if std::time::Instant::now().duration_since(last_received) > incoming_timeout.unwrap() {
                    return Err(StompError::Protocol("Heartbeat timeout".to_string()));
                }
            }

            // Incoming command from client handle
            opt_cmd = cmd_future => {
                if let Some(cmd) = opt_cmd {
                    match cmd {
                        ClientCmd::Send { request, resp } => {
                            let frame = send_request_to_frame(request);
                            let res = writer.send(frame).await.map_err(StompError::Io);
                            let _ = resp.send(res);
                        }
                        ClientCmd::Subscribe { request, sender, resp } => {
                            let id = request.id.clone().unwrap_or_else(|| "1".to_string());
                            subscriptions.insert(id.clone(), sender);
                            let subscribe_frame = subscribe_request_to_frame(request, id);
                            let res = writer.send(subscribe_frame).await.map_err(StompError::Io);
                            let _ = resp.send(res);
                        }
                        ClientCmd::Unsubscribe { id, resp } => {
                            subscriptions.remove(&id);
                            let unsubscribe_frame = StompFrame {
                                command: Cow::Borrowed("UNSUBSCRIBE"),
                                headers: vec![("id".to_string(), id)],
                                body: None,
                            };
                            let res = writer.send(unsubscribe_frame).await.map_err(StompError::Io);
                            let _ = resp.send(res);
                        }
                        ClientCmd::Ack { request, resp } => {
                            let headers = match ack_headers(request, negotiated_version) {
                                Ok(headers) => headers,
                                Err(err) => {
                                    let _ = resp.send(Err(err));
                                    continue;
                                }
                            };
                            let frame = StompFrame {
                                command: Cow::Borrowed("ACK"),
                                headers,
                                body: None,
                            };
                            let res = writer.send(frame).await.map_err(StompError::Io);
                            let _ = resp.send(res);
                        }
                        ClientCmd::Nack { request, resp } => {
                            let headers = match nack_headers(request, negotiated_version) {
                                Ok(headers) => headers,
                                Err(err) => {
                                    let _ = resp.send(Err(err));
                                    continue;
                                }
                            };
                            let frame = StompFrame {
                                command: Cow::Borrowed("NACK"),
                                headers,
                                body: None,
                            };
                            let res = writer.send(frame).await.map_err(StompError::Io);
                            let _ = resp.send(res);
                        }
                        ClientCmd::Begin { transaction_id, resp } => {
                            let frame = StompFrame {
                                command: Cow::Borrowed("BEGIN"),
                                headers: vec![("transaction".to_string(), transaction_id)],
                                body: None,
                            };
                            let res = writer.send(frame).await.map_err(StompError::Io);
                            let _ = resp.send(res);
                        }
                        ClientCmd::Commit { transaction_id, resp } => {
                            let frame = StompFrame {
                                command: Cow::Borrowed("COMMIT"),
                                headers: vec![("transaction".to_string(), transaction_id)],
                                body: None,
                            };
                            let res = writer.send(frame).await.map_err(StompError::Io);
                            let _ = resp.send(res);
                        }
                        ClientCmd::Abort { transaction_id, resp } => {
                            let frame = StompFrame {
                                command: Cow::Borrowed("ABORT"),
                                headers: vec![("transaction".to_string(), transaction_id)],
                                body: None,
                            };
                            let res = writer.send(frame).await.map_err(StompError::Io);
                            let _ = resp.send(res);
                        }
                        ClientCmd::Disconnect { resp } => {
                            let frame = StompFrame {
                                command: Cow::Borrowed("DISCONNECT"),
                                headers: vec![],
                                body: None,
                            };
                            let res = writer.send(frame).await.map_err(StompError::Io);
                            let should_close = res.is_ok();
                            let _ = resp.send(res);
                            if should_close {
                                return Ok(());
                            }
                        }
                    }
                } else {
                    return Ok(());
                }
            }

            // Incoming frame from socket
            res = read_future => {
                match res {
                    Some(Ok(frame)) => {
                        last_received = std::time::Instant::now();
                        if frame.command == "MESSAGE" {
                            if let Some(id) = frame.get_header("subscription") {
                                if let Some(sender) = subscriptions.get(id) {
                                    let _ = sender.send(frame);
                                }
                            }
                        } else if frame.command == "ERROR" {
                            return Err(StompError::Protocol(format!(
                                "Server error: {:?}",
                                frame
                            )));
                        }
                    }
                    Some(Err(e)) => return Err(StompError::Io(e)),
                    None => return Err(StompError::Disconnected),
                }
            }
        }
    }
}

fn send_request_to_frame(request: SendRequest) -> StompFrame<'static> {
    let mut headers = request.headers;
    upsert_header(&mut headers, "destination", request.destination);
    StompFrame {
        command: Cow::Borrowed("SEND"),
        headers,
        body: Some(Cow::Owned(request.body)),
    }
}

fn subscribe_request_to_frame(request: SubscribeRequest, id: String) -> StompFrame<'static> {
    let mut headers = request.headers;
    upsert_header(&mut headers, "id", id);
    upsert_header(&mut headers, "destination", request.destination);
    if !headers.iter().any(|(key, _)| key == "ack") {
        headers.push((
            "ack".to_string(),
            AckMode::Auto.as_header_value().to_string(),
        ));
    }
    StompFrame {
        command: Cow::Borrowed("SUBSCRIBE"),
        headers,
        body: None,
    }
}
