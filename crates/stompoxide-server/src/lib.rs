use std::{
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use stompoxide_codec::{StompCodec, StompFrame, StompVersion};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpListener,
    sync::{RwLock, Semaphore, mpsc, oneshot},
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tower::Service;
use uuid::Uuid;

#[cfg(feature = "websocket")]
mod http;
#[cfg(feature = "websocket")]
#[doc(hidden)]
pub mod ws;

#[cfg(feature = "websocket")]
pub use http::StompWebSocketService;

type WriteResult = Result<(), std::io::Error>;

pub const STOMP_SUBPROTOCOLS: [&str; 3] = ["v12.stomp", "v11.stomp", "v10.stomp"];

pub fn select_stomp_subprotocol(header_value: Option<&str>) -> Option<&'static str> {
    let client_protocols: Vec<&str> = header_value
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .collect();

    if client_protocols.contains(&STOMP_SUBPROTOCOLS[0]) {
        Some(STOMP_SUBPROTOCOLS[0])
    } else if client_protocols.contains(&STOMP_SUBPROTOCOLS[1]) {
        Some(STOMP_SUBPROTOCOLS[1])
    } else if client_protocols.contains(&STOMP_SUBPROTOCOLS[2]) {
        Some(STOMP_SUBPROTOCOLS[2])
    } else {
        None
    }
}

enum WriteCommand {
    Frame(StompFrame<'static>, Option<oneshot::Sender<WriteResult>>),
    Heartbeat,
}

#[derive(Debug, Clone)]
pub enum ServerFrame {
    Owned(StompFrame<'static>),
    Borrowed(Arc<StompFrame<'static>>, Option<String>, Option<String>),
}

pub struct SubscriptionInfo {
    pub conn_id: Uuid,
    pub sub_id: String,
    pub sender: mpsc::UnboundedSender<ServerFrame>,
    pub ack_mode: String,
    pub version: StompVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DestinationKind {
    Topic,
    Queue,
}

#[derive(Clone)]
struct PendingAckInfo {
    conn_id: Uuid,
    sub_id: String,
    destination: String,
    frame: Arc<StompFrame<'static>>,
    ack_mode: String,
    time_sent: std::time::Instant,
    message_id: String,
}

struct AckTarget<'a> {
    id: &'a str,
    subscription: Option<&'a str>,
    version: StompVersion,
}

fn find_pending_ack(
    pending_acks: &HashMap<String, PendingAckInfo>,
    conn_id: Uuid,
    target: AckTarget<'_>,
) -> Option<String> {
    match target.version {
        StompVersion::V1_2 => pending_acks
            .get_key_value(target.id)
            .filter(|(_, info)| info.conn_id == conn_id)
            .map(|(delivery_id, _)| delivery_id.clone()),
        StompVersion::V1_1 => {
            let subscription = target.subscription?;
            pending_acks
                .iter()
                .find(|(_, info)| {
                    info.conn_id == conn_id
                        && info.message_id == target.id
                        && info.sub_id == subscription
                })
                .map(|(delivery_id, _)| delivery_id.clone())
        }
        StompVersion::V1_0 => pending_acks
            .iter()
            .find(|(_, info)| info.conn_id == conn_id && info.message_id == target.id)
            .map(|(delivery_id, _)| delivery_id.clone()),
    }
}

#[derive(Default)]
struct RouterState {
    topics: HashMap<String, Vec<SubscriptionInfo>>,
    queues: HashMap<String, QueueInfo>,
    pending_acks: HashMap<String, PendingAckInfo>,
}

#[derive(Default)]
struct QueueInfo {
    subscriptions: Vec<SubscriptionInfo>,
    next_index: usize,
}

#[derive(Clone, Default)]
pub struct PubSubRouter {
    state: Arc<RwLock<RouterState>>,
}

impl PubSubRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn subscribe(
        &self,
        conn_id: Uuid,
        sub_id: String,
        destination: String,
        ack_mode: String,
        version: StompVersion,
        sender: mpsc::UnboundedSender<ServerFrame>,
    ) -> Result<(), &'static str> {
        let kind = classify_destination(&destination).ok_or("Unknown destination")?;
        if kind == DestinationKind::Queue && contains_wildcard(&destination) {
            return Err("Queue subscriptions must use an exact destination");
        }

        let mut state = self.state.write().await;
        let subscription = SubscriptionInfo {
            conn_id,
            sub_id,
            sender,
            ack_mode,
            version,
        };
        match kind {
            DestinationKind::Topic => state
                .topics
                .entry(destination)
                .or_default()
                .push(subscription),
            DestinationKind::Queue => state
                .queues
                .entry(destination)
                .or_default()
                .subscriptions
                .push(subscription),
        }

        Ok(())
    }

    pub async fn unsubscribe(&self, conn_id: Uuid, sub_id: &str) {
        let mut state = self.state.write().await;
        for subs in state.topics.values_mut() {
            subs.retain(|sub| !(sub.conn_id == conn_id && sub.sub_id == sub_id));
        }
        for queue in state.queues.values_mut() {
            queue
                .subscriptions
                .retain(|sub| !(sub.conn_id == conn_id && sub.sub_id == sub_id));
            if !queue.subscriptions.is_empty() {
                queue.next_index %= queue.subscriptions.len();
            } else {
                queue.next_index = 0;
            }
        }
    }

    pub async fn clean_connection(&self, conn_id: Uuid) {
        let mut to_republish = Vec::new();
        {
            let mut state = self.state.write().await;
            for subs in state.topics.values_mut() {
                subs.retain(|sub| sub.conn_id != conn_id);
            }
            for queue in state.queues.values_mut() {
                queue.subscriptions.retain(|sub| sub.conn_id != conn_id);
                if !queue.subscriptions.is_empty() {
                    queue.next_index %= queue.subscriptions.len();
                } else {
                    queue.next_index = 0;
                }
            }

            let mut conn_acks = Vec::new();
            state.pending_acks.retain(|_, v| {
                if v.conn_id == conn_id {
                    conn_acks.push(v.clone());
                    false
                } else {
                    true
                }
            });
            for ack in conn_acks {
                to_republish.push((ack.destination, ack.frame));
            }
        }

        for (destination, frame) in to_republish {
            let _ = self.publish_shared(&destination, frame).await;
        }
    }

    async fn handle_ack(&self, conn_id: Uuid, target: AckTarget<'_>) {
        let mut state = self.state.write().await;
        if let Some(del_id) = find_pending_ack(&state.pending_acks, conn_id, target) {
            if let Some(pending) = state.pending_acks.remove(&del_id) {
                if pending.ack_mode == "client" {
                    let time_sent = pending.time_sent;
                    let sub_id = pending.sub_id;
                    state.pending_acks.retain(|_, v| {
                        !(v.conn_id == conn_id && v.sub_id == sub_id && v.time_sent <= time_sent)
                    });
                }
            }
        }
    }

    async fn handle_nack(&self, conn_id: Uuid, target: AckTarget<'_>) {
        let mut to_republish = Vec::new();
        {
            let mut state = self.state.write().await;
            if let Some(del_id) = find_pending_ack(&state.pending_acks, conn_id, target) {
                if let Some(pending) = state.pending_acks.remove(&del_id) {
                    if pending.ack_mode == "client" {
                        let time_sent = pending.time_sent;
                        let sub_id = pending.sub_id.clone();
                        let mut rejected = Vec::new();
                        state.pending_acks.retain(|_, v| {
                            if v.conn_id == conn_id
                                && v.sub_id == sub_id
                                && v.time_sent <= time_sent
                            {
                                rejected.push(v.clone());
                                false
                            } else {
                                true
                            }
                        });
                        rejected.push(pending);
                        for r in rejected {
                            to_republish.push((r.destination, r.frame));
                        }
                    } else {
                        to_republish.push((pending.destination, pending.frame));
                    }
                }
            }
        }
        for (destination, frame) in to_republish {
            let _ = self.publish_shared(&destination, frame).await;
        }
    }

    pub async fn publish(
        &self,
        destination: &str,
        frame: StompFrame<'static>,
    ) -> Result<(), &'static str> {
        self.publish_shared(destination, Arc::new(frame)).await
    }

    pub async fn publish_shared(
        &self,
        destination: &str,
        frame: Arc<StompFrame<'static>>,
    ) -> Result<(), &'static str> {
        let kind = classify_destination(destination).ok_or("Unknown destination")?;
        let mut state = self.state.write().await;
        let RouterState {
            topics,
            queues,
            pending_acks,
        } = &mut *state;
        match kind {
            DestinationKind::Topic => {
                let mut target_subs = Vec::new();
                for (pattern, subs) in topics.iter() {
                    if matches_destination(pattern, destination) {
                        for sub in subs {
                            target_subs.push((
                                sub.conn_id,
                                sub.sub_id.clone(),
                                sub.ack_mode.clone(),
                                sub.version,
                                sub.sender.clone(),
                            ));
                        }
                    }
                }
                for (conn_id, sub_id, ack_mode, version, sender) in target_subs {
                    let mut ack_id = None;
                    if ack_mode == "client" || ack_mode == "client-individual" {
                        let delivery_id = Uuid::new_v4().to_string();
                        if version == StompVersion::V1_2 {
                            ack_id = Some(delivery_id.clone());
                        }
                        let message_id = frame
                            .get_header("message-id")
                            .map(|s| s.to_string())
                            .unwrap_or_default();

                        pending_acks.insert(
                            delivery_id,
                            PendingAckInfo {
                                conn_id,
                                sub_id: sub_id.clone(),
                                destination: destination.to_string(),
                                frame: frame.clone(),
                                ack_mode,
                                time_sent: std::time::Instant::now(),
                                message_id,
                            },
                        );
                    }
                    let _ = sender.send(ServerFrame::Borrowed(frame.clone(), Some(sub_id), ack_id));
                }
            }
            DestinationKind::Queue => {
                if let Some(queue) = queues.get_mut(destination) {
                    if queue.subscriptions.is_empty() {
                        return Ok(());
                    }
                    let index = queue.next_index % queue.subscriptions.len();
                    queue.next_index = (index + 1) % queue.subscriptions.len();
                    let sub = &queue.subscriptions[index];
                    let mut ack_id = None;
                    if sub.ack_mode == "client" || sub.ack_mode == "client-individual" {
                        let delivery_id = Uuid::new_v4().to_string();
                        if sub.version == StompVersion::V1_2 {
                            ack_id = Some(delivery_id.clone());
                        }
                        let message_id = frame
                            .get_header("message-id")
                            .map(|s| s.to_string())
                            .unwrap_or_default();

                        pending_acks.insert(
                            delivery_id,
                            PendingAckInfo {
                                conn_id: sub.conn_id,
                                sub_id: sub.sub_id.clone(),
                                destination: destination.to_string(),
                                frame: frame.clone(),
                                ack_mode: sub.ack_mode.clone(),
                                time_sent: std::time::Instant::now(),
                                message_id,
                            },
                        );
                    }
                    let _ = sub.sender.send(ServerFrame::Borrowed(
                        frame,
                        Some(sub.sub_id.clone()),
                        ack_id,
                    ));
                }
            }
        }
        Ok(())
    }
}

fn classify_destination(destination: &str) -> Option<DestinationKind> {
    if destination.starts_with("/topic/") {
        Some(DestinationKind::Topic)
    } else if destination.starts_with("/queue/") {
        Some(DestinationKind::Queue)
    } else {
        None
    }
}

fn contains_wildcard(destination: &str) -> bool {
    destination
        .split('/')
        .any(|segment| segment == "*" || segment == "**")
}

fn matches_destination(pattern: &str, destination: &str) -> bool {
    let pat_segs: Vec<&str> = pattern.split('/').collect();
    let dest_segs: Vec<&str> = destination.split('/').collect();
    matches_destination_segments(&pat_segs, &dest_segs)
}

fn matches_destination_segments(pattern: &[&str], destination: &[&str]) -> bool {
    if pattern.is_empty() {
        return destination.is_empty();
    }

    match pattern[0] {
        "**" => {
            matches_destination_segments(&pattern[1..], destination)
                || (!destination.is_empty()
                    && matches_destination_segments(pattern, &destination[1..]))
        }
        "*" => {
            !destination.is_empty()
                && matches_destination_segments(&pattern[1..], &destination[1..])
        }
        segment => {
            destination.first() == Some(&segment)
                && matches_destination_segments(&pattern[1..], &destination[1..])
        }
    }
}

fn ack_id_and_subscription<'a>(
    frame: &'a StompFrame<'_>,
    version: StompVersion,
) -> Result<AckTarget<'a>, String> {
    let (id, subscription) = match version {
        StompVersion::V1_0 => {
            if frame.command == "NACK" {
                return Err("STOMP 1.0 does not support NACK".to_string());
            }
            frame
                .get_header("message-id")
                .map(|id| (id, None))
                .ok_or_else(|| format!("Missing message-id header in {}", frame.command))
        }
        StompVersion::V1_1 => {
            let message_id = frame
                .get_header("message-id")
                .ok_or_else(|| format!("Missing message-id header in {}", frame.command))?;
            let subscription = frame.get_header("subscription").ok_or_else(|| {
                format!(
                    "Missing subscription header in {} for STOMP 1.1",
                    frame.command
                )
            })?;
            Ok((message_id, Some(subscription)))
        }
        StompVersion::V1_2 => frame
            .get_header("id")
            .map(|id| (id, frame.get_header("subscription")))
            .ok_or_else(|| format!("Missing id header in {}", frame.command)),
    }?;

    Ok(AckTarget {
        id,
        subscription,
        version,
    })
}

pub type StompAuthenticator = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

pub const DEFAULT_MAX_CONNECTIONS: usize = 1024;

#[derive(Clone)]
struct ServerConfig {
    limiter: Arc<Semaphore>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self::with_max_connections(DEFAULT_MAX_CONNECTIONS)
    }
}

impl ServerConfig {
    fn with_max_connections(max_connections: usize) -> Self {
        Self {
            limiter: Arc::new(Semaphore::new(max_connections)),
        }
    }
}

/// The main STOMP server instance.
///
/// # Examples
/// ```
/// use stompoxide_server::StompServer;
///
/// let server = StompServer::new();
/// let router = server.router();
/// ```
#[derive(Clone, Default)]
pub struct StompServer {
    router: PubSubRouter,
    authenticator: Option<StompAuthenticator>,
    config: ServerConfig,
}

impl StompServer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_authenticator<F>(mut self, authenticator: F) -> Self
    where
        F: Fn(&str, &str) -> bool + Send + Sync + 'static,
    {
        self.authenticator = Some(Arc::new(authenticator));
        self
    }

    pub fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.config = ServerConfig::with_max_connections(max_connections);
        self
    }

    pub fn router(&self) -> &PubSubRouter {
        &self.router
    }

    pub fn connection_service(&self) -> StompConnectionService {
        StompConnectionService::new(self.clone())
    }

    #[cfg(feature = "websocket")]
    pub fn websocket_service(&self) -> StompWebSocketService {
        StompWebSocketService::new(self.connection_service())
    }

    pub async fn listen_tcp(&self, addr: &str) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(addr).await?;
        log::info!("STOMP Server listening on TCP {}", addr);
        loop {
            let permit = self.config.limiter.clone().acquire_owned().await;
            let Ok(permit) = permit else {
                return Ok(());
            };
            match listener.accept().await {
                Ok((socket, _)) => {
                    let server = self.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        server.handle_connection(socket).await;
                    });
                }
                Err(e) => {
                    drop(permit);
                    log::error!("TCP accept error: {:?}", e);
                }
            }
        }
    }

    pub async fn handle_connection<S>(&self, stream: S)
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let conn_id = Uuid::new_v4();
        let (reader, writer) = tokio::io::split(stream);
        let mut framed_read = FramedRead::new(reader, StompCodec::default());
        let mut framed_write = FramedWrite::new(writer, StompCodec::default());

        // 1. Wait for CONNECT / STOMP frame
        let connect_frame = match framed_read.next().await {
            Some(Ok(frame)) => {
                log::info!(
                    "New connection {}: Received frame: {:?}",
                    conn_id,
                    frame.command
                );
                if frame.command != "CONNECT" && frame.command != "STOMP" {
                    log::warn!(
                        "Connection {}: Unexpected initial command {:?}",
                        conn_id,
                        frame.command
                    );
                    let _ = send_error(&mut framed_write, "Missing CONNECT frame").await;
                    return;
                }
                frame
            }
            Some(Err(e)) => {
                log::error!("Connection {conn_id}: Error reading connect frame: {e:?}",);
                return;
            }
            None => {
                log::warn!(
                    "Connection {}: Closed before sending CONNECT frame",
                    conn_id
                );
                return;
            }
        };

        // Authenticate connection if an authenticator is configured.
        if let Some(ref auth) = self.authenticator {
            let login = connect_frame.get_header("login").unwrap_or("");
            let passcode = connect_frame.get_header("passcode").unwrap_or("");
            if !auth(login, passcode) {
                log::warn!(
                    "Connection {}: Authentication failed for login '{}'",
                    conn_id,
                    login
                );
                let _ = send_error(&mut framed_write, "Authentication failed").await;
                return;
            }
        }

        // Negotiate version.
        let version_str = if let Some(val) = connect_frame.get_header("accept-version") {
            let versions: Vec<&str> = val.split(',').map(|s| s.trim()).collect();
            if versions.contains(&"1.2") {
                Some("1.2")
            } else if versions.contains(&"1.1") {
                Some("1.1")
            } else if versions.contains(&"1.0") {
                Some("1.0")
            } else {
                None
            }
        } else {
            Some("1.0") // Default to 1.0 if accept-version is missing
        };

        let negotiated_version_str = match version_str {
            Some(v) => v,
            None => {
                let mut err_frame = create_error_frame("Supported versions: 1.0, 1.1, 1.2");
                err_frame
                    .headers
                    .push(("version".to_string(), "1.2,1.1,1.0".to_string()));
                let _ = framed_write.send(err_frame).await;
                return;
            }
        };

        let negotiated_version = match negotiated_version_str {
            "1.0" => StompVersion::V1_0,
            "1.1" => StompVersion::V1_1,
            "1.2" => StompVersion::V1_2,
            _ => StompVersion::V1_2,
        };

        framed_read.decoder_mut().version = negotiated_version;
        framed_write.encoder_mut().version = negotiated_version;
        // NOTE: According to the STOMP 1.1 / 1.2 specifications, the "host" header is REQUIRED
        // in CONNECT/STOMP frames. However, popular client libraries (such as @stomp/stompjs)
        // do not send it by default, and major message brokers (like RabbitMQ and ActiveMQ)
        // are lenient and do not enforce it. To prevent compatibility issues for users, we
        // intentionally skip this strict validation, keeping it commented out for reference.
        /*
        match negotiated_version {
            StompVersion::V1_0 => (),
            StompVersion::V1_1 | StompVersion::V1_2 => {
                if connect_frame.get_header("host").is_none() {
                    let _ = send_error(&mut framed_write, "Missing required host header").await;
                    return;
                }
            }
        }
        */

        // Negotiate heartbeat.
        let mut client_cx = 0;
        let mut client_cy = 0;
        if let Some(val) = connect_frame.get_header("heart-beat") {
            let parts: Vec<&str> = val.split(',').collect();
            if parts.len() == 2 {
                client_cx = parts[0].parse().unwrap_or(0);
                client_cy = parts[1].parse().unwrap_or(0);
            }
        }

        // Server preferences: send every 5000ms, expect every 5000ms
        let server_cx = 5000;
        let server_cy = 5000;

        let mut outgoing_hb = if server_cx > 0 && client_cy > 0 {
            std::cmp::max(server_cx, client_cy)
        } else {
            0
        };

        let mut incoming_hb = if server_cy > 0 && client_cx > 0 {
            std::cmp::max(server_cy, client_cx)
        } else {
            0
        };

        if negotiated_version == StompVersion::V1_0 {
            outgoing_hb = 0;
            incoming_hb = 0;
        }

        // Send CONNECTED frame
        let mut connected_headers = Vec::new();
        if negotiated_version != StompVersion::V1_0 {
            connected_headers.push(("version".to_string(), negotiated_version_str.to_string()));
            connected_headers.push((
                "heart-beat".to_string(),
                format!("{},{}", server_cx, server_cy),
            ));
            if negotiated_version == StompVersion::V1_2 {
                connected_headers.push(("server".to_string(), "stompoxide/0.1.0".to_string()));
            }
        }
        connected_headers.push(("session".to_string(), conn_id.to_string()));

        let connected_frame = StompFrame {
            command: Cow::Borrowed("CONNECTED"),
            headers: connected_headers,
            body: None,
        };

        log::info!(
            "Connection {}: Negotiated STOMP version {}, outgoing heartbeat: {}ms, incoming heartbeat: {}ms",
            conn_id,
            negotiated_version_str,
            outgoing_hb,
            incoming_hb
        );

        if let Err(e) = framed_write.send(connected_frame).await {
            log::error!(
                "Connection {}: Error sending CONNECTED frame: {:?}",
                conn_id,
                e
            );
            return;
        }

        // Channels for outgoing frames
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerFrame>();
        let (write_cmd_tx, mut write_cmd_rx) = mpsc::channel::<WriteCommand>(100);

        // Spawn a background write worker to serialize frames to the socket
        let write_worker = tokio::spawn(async move {
            let mut write_buf = bytes::BytesMut::new();
            loop {
                tokio::select! {
                    Some(frame) = out_rx.recv() => {
                        log::info!("Connection {}: Sending frame: {:?}", conn_id, frame);
                        write_buf.clear();
                        serialize_server_frame(&frame, negotiated_version, &mut write_buf);
                        if let Err(e) = framed_write.get_mut().write_all(&write_buf).await {
                            log::error!("Connection {}: Writer worker error: {:?}", conn_id, e);
                            break;
                        }
                        if let Err(e) = framed_write.get_mut().flush().await {
                            log::error!("Connection {}: Writer worker flush error: {:?}", conn_id, e);
                            break;
                        }
                    }
                    Some(cmd) = write_cmd_rx.recv() => {
                        match cmd {
                            WriteCommand::Frame(frame, completion) => {
                                log::info!("Connection {}: Sending command frame: {:?}", conn_id, frame.command);
                                let result = framed_write.send(frame).await;
                                let should_break = result.is_err();
                                if let Some(completion) = completion {
                                    let _ = completion.send(result);
                                } else if let Err(e) = result {
                                    log::error!("Connection {}: Writer worker command error: {:?}", conn_id, e);
                                }
                                if should_break {
                                    break;
                                }
                            }
                            WriteCommand::Heartbeat => {
                                // Send raw EOL heartbeat directly and flush the stream
                                log::info!("Connection {}: Sending EOL heartbeat", conn_id);
                                if let Err(e) = framed_write.get_mut().write_all(b"\n").await {
                                    log::error!("Connection {}: Writer heartbeat write error: {:?}", conn_id, e);
                                    break;
                                }
                                if let Err(e) = framed_write.get_mut().flush().await {
                                    log::error!("Connection {}: Writer heartbeat flush error: {:?}", conn_id, e);
                                    break;
                                }
                            }
                        }
                    }
                    else => break,
                }
            }
        });

        // Outgoing heartbeat timer task
        if outgoing_hb > 0 {
            let write_cmd_tx = write_cmd_tx.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_millis(outgoing_hb as u64));
                loop {
                    interval.tick().await;
                    if write_cmd_tx.send(WriteCommand::Heartbeat).await.is_err() {
                        break;
                    }
                }
            });
        }

        // Incoming heartbeat check
        let incoming_timeout = if incoming_hb > 0 {
            Some(Duration::from_millis((incoming_hb as f64 * 1.5) as u64))
        } else {
            None
        };

        let router = self.router.clone();

        let mut transactions: HashMap<String, Vec<StompFrame<'static>>> = HashMap::new();

        // Read connection event loop
        loop {
            let read_future = framed_read.next();
            let res = async {
                if let Some(timeout) = incoming_timeout {
                    tokio::time::timeout(timeout, read_future).await
                } else {
                    Ok(read_future.await)
                }
            }
            .await;

            match res {
                Ok(Some(Ok(frame))) => {
                    log::info!(
                        "Connection {}: Received frame: {:?}",
                        conn_id,
                        frame.command
                    );
                    let receipt_id = frame.get_header("receipt").map(|s| s.to_string());

                    // Validate ACK / NACK headers before immediate handling or transaction buffering.
                    if frame.command == "ACK" || frame.command == "NACK" {
                        if let Err(message) = ack_id_and_subscription(&frame, negotiated_version) {
                            let _ = send_command_frame(
                                &write_cmd_tx,
                                create_error_frame(&message),
                                true,
                            )
                            .await;
                            break;
                        }
                    }

                    let mut should_process_normally = true;
                    let mut should_send_receipt = true;

                    let is_transactional_command = frame.command == "SEND"
                        || frame.command == "ACK"
                        || frame.command == "NACK";
                    if is_transactional_command {
                        if let Some(t_id) = frame.get_header("transaction") {
                            if !transactions.contains_key(t_id) {
                                let _ = send_command_frame(
                                    &write_cmd_tx,
                                    create_error_frame(&format!(
                                        "Transaction '{}' not active",
                                        t_id
                                    )),
                                    true,
                                )
                                .await;
                                break;
                            } else {
                                transactions
                                    .get_mut(t_id)
                                    .unwrap()
                                    .push(frame.clone().into_owned());
                                should_process_normally = false;
                                should_send_receipt = false;
                            }
                        }
                    }

                    if should_process_normally {
                        let process_result = match frame.command.as_ref() {
                            "BEGIN" => {
                                self.handle_begin_cmd(&frame, &write_cmd_tx, &mut transactions)
                                    .await
                            }
                            "COMMIT" => {
                                self.handle_commit_cmd(
                                    &frame,
                                    &write_cmd_tx,
                                    &mut transactions,
                                    conn_id,
                                    negotiated_version,
                                )
                                .await
                            }
                            "ABORT" => {
                                self.handle_abort_cmd(&frame, &write_cmd_tx, &mut transactions)
                                    .await
                            }
                            "SUBSCRIBE" => {
                                self.handle_subscribe_cmd(
                                    &frame,
                                    &write_cmd_tx,
                                    conn_id,
                                    negotiated_version,
                                    &out_tx,
                                )
                                .await
                            }
                            "UNSUBSCRIBE" => {
                                self.handle_unsubscribe_cmd(&frame, &write_cmd_tx, conn_id)
                                    .await
                            }
                            "SEND" => self.handle_send_cmd(&frame, &write_cmd_tx).await,
                            "DISCONNECT" => {
                                if let Some(r_id) = receipt_id.clone() {
                                    let _ = send_command_frame(
                                        &write_cmd_tx,
                                        create_receipt_frame(r_id),
                                        true,
                                    )
                                    .await;
                                }
                                Err(())
                            }
                            "ACK" => {
                                let target = ack_id_and_subscription(&frame, negotiated_version)
                                    .expect("ACK was validated before handling");
                                router.handle_ack(conn_id, target).await;
                                Ok(())
                            }
                            "NACK" => {
                                let target = ack_id_and_subscription(&frame, negotiated_version)
                                    .expect("NACK was validated before handling");
                                router.handle_nack(conn_id, target).await;
                                Ok(())
                            }
                            "HEARTBEAT" => Ok(()),
                            command => {
                                let _ = send_command_frame(
                                    &write_cmd_tx,
                                    create_error_frame(&format!(
                                        "Unsupported command: {}",
                                        command
                                    )),
                                    true,
                                )
                                .await;
                                Err(())
                            }
                        };
                        if process_result.is_err() {
                            break;
                        }
                    }

                    // Send receipt if requested (except for DISCONNECT which we handled above)
                    if frame.command != "DISCONNECT" && should_send_receipt {
                        if let Some(r_id) = receipt_id {
                            let _ = send_command_frame(
                                &write_cmd_tx,
                                create_receipt_frame(r_id),
                                false,
                            )
                            .await;
                        }
                    }
                }
                Ok(Some(Err(e))) => {
                    log::error!("Connection read error: {:?}", e);
                    break;
                }
                Ok(None) => break,
                Err(_) => {
                    log::warn!("Heartbeat timeout from client {:?}", conn_id);
                    break;
                }
            }
        }

        // Clean up connection
        router.clean_connection(conn_id).await;
        write_worker.abort();
    }

    async fn handle_begin_cmd(
        &self,
        frame: &StompFrame<'_>,
        write_cmd_tx: &mpsc::Sender<WriteCommand>,
        transactions: &mut HashMap<String, Vec<StompFrame<'static>>>,
    ) -> Result<(), ()> {
        let t_id = match frame.get_header("transaction") {
            Some(id) => id.to_string(),
            None => {
                let _ = send_command_frame(
                    write_cmd_tx,
                    create_error_frame("Missing transaction header"),
                    true,
                )
                .await;
                return Err(());
            }
        };
        if transactions.contains_key(&t_id) {
            let _ = send_command_frame(
                write_cmd_tx,
                create_error_frame(&format!("Transaction '{}' already active", t_id)),
                true,
            )
            .await;
            return Err(());
        }
        transactions.insert(t_id, Vec::new());
        Ok(())
    }

    async fn handle_commit_cmd(
        &self,
        frame: &StompFrame<'_>,
        write_cmd_tx: &mpsc::Sender<WriteCommand>,
        transactions: &mut HashMap<String, Vec<StompFrame<'static>>>,
        conn_id: Uuid,
        negotiated_version: StompVersion,
    ) -> Result<(), ()> {
        let t_id = match frame.get_header("transaction") {
            Some(id) => id.to_string(),
            None => {
                let _ = send_command_frame(
                    write_cmd_tx,
                    create_error_frame("Missing transaction header"),
                    true,
                )
                .await;
                return Err(());
            }
        };
        if let Some(buffered_frames) = transactions.remove(&t_id) {
            let mut commit_failed = false;
            for f in buffered_frames {
                let sub_receipt = f.get_header("receipt").map(|s| s.to_string());
                match f.command.as_ref() {
                    "SEND" => {
                        let destination = f.get_header("destination").map(|s| s.to_string());
                        if let Some(dest) = destination {
                            let mut message_frame = StompFrame {
                                command: Cow::Borrowed("MESSAGE"),
                                headers: f
                                    .headers
                                    .iter()
                                    .filter(|(k, _)| {
                                        k != "receipt" && k != "ack" && k != "transaction"
                                    })
                                    .cloned()
                                    .collect(),
                                body: f.body.map(|b| Cow::Owned(b.into_owned())),
                            };
                            let message_id = Uuid::new_v4().to_string();
                            message_frame
                                .headers
                                .push(("message-id".to_string(), message_id));
                            if let Err(message) = self.router.publish(&dest, message_frame).await {
                                let _ = send_command_frame(
                                    write_cmd_tx,
                                    create_error_frame(message),
                                    true,
                                )
                                .await;
                                commit_failed = true;
                                break;
                            }
                        } else {
                            let _ = send_command_frame(
                                write_cmd_tx,
                                create_error_frame("Missing destination header"),
                                true,
                            )
                            .await;
                            commit_failed = true;
                            break;
                        }
                    }
                    "ACK" => match ack_id_and_subscription(&f, negotiated_version) {
                        Ok(target) => {
                            self.router.handle_ack(conn_id, target).await;
                        }
                        Err(message) => {
                            let _ = send_command_frame(
                                write_cmd_tx,
                                create_error_frame(&message),
                                true,
                            )
                            .await;
                            commit_failed = true;
                            break;
                        }
                    },
                    "NACK" => match ack_id_and_subscription(&f, negotiated_version) {
                        Ok(target) => {
                            self.router.handle_nack(conn_id, target).await;
                        }
                        Err(message) => {
                            let _ = send_command_frame(
                                write_cmd_tx,
                                create_error_frame(&message),
                                true,
                            )
                            .await;
                            commit_failed = true;
                            break;
                        }
                    },
                    _ => {}
                }
                if let Some(r_id) = sub_receipt {
                    let _ =
                        send_command_frame(write_cmd_tx, create_receipt_frame(r_id), false).await;
                }
            }
            if commit_failed {
                return Err(());
            }
        } else {
            let _ = send_command_frame(
                write_cmd_tx,
                create_error_frame(&format!("Transaction '{}' not active", t_id)),
                true,
            )
            .await;
            return Err(());
        }
        Ok(())
    }

    async fn handle_abort_cmd(
        &self,
        frame: &StompFrame<'_>,
        write_cmd_tx: &mpsc::Sender<WriteCommand>,
        transactions: &mut HashMap<String, Vec<StompFrame<'static>>>,
    ) -> Result<(), ()> {
        let t_id = match frame.get_header("transaction") {
            Some(id) => id.to_string(),
            None => {
                let _ = send_command_frame(
                    write_cmd_tx,
                    create_error_frame("Missing transaction header"),
                    true,
                )
                .await;
                return Err(());
            }
        };
        if transactions.remove(&t_id).is_none() {
            let _ = send_command_frame(
                write_cmd_tx,
                create_error_frame(&format!("Transaction '{}' not active", t_id)),
                true,
            )
            .await;
            return Err(());
        }
        Ok(())
    }

    async fn handle_subscribe_cmd(
        &self,
        frame: &StompFrame<'_>,
        write_cmd_tx: &mpsc::Sender<WriteCommand>,
        conn_id: Uuid,
        negotiated_version: StompVersion,
        out_tx: &mpsc::UnboundedSender<ServerFrame>,
    ) -> Result<(), ()> {
        let id = frame.get_header("id").map(|s| s.to_string());
        let destination = frame.get_header("destination").map(|s| s.to_string());
        let ack_mode = frame.get_header("ack").unwrap_or("auto").to_string();

        if negotiated_version == StompVersion::V1_0 && ack_mode == "client-individual" {
            let _ = send_command_frame(
                write_cmd_tx,
                create_error_frame("STOMP 1.0 does not support client-individual ack mode"),
                true,
            )
            .await;
            return Err(());
        }

        if let (Some(sub_id), Some(dest)) = (id, destination) {
            if let Err(message) = self
                .router
                .subscribe(
                    conn_id,
                    sub_id,
                    dest,
                    ack_mode,
                    negotiated_version,
                    out_tx.clone(),
                )
                .await
            {
                let _ = send_command_frame(write_cmd_tx, create_error_frame(message), true).await;
                return Err(());
            }
        } else {
            let _ = send_command_frame(
                write_cmd_tx,
                create_error_frame("Missing SUBSCRIBE headers"),
                true,
            )
            .await;
            return Err(());
        }
        Ok(())
    }

    async fn handle_unsubscribe_cmd(
        &self,
        frame: &StompFrame<'_>,
        write_cmd_tx: &mpsc::Sender<WriteCommand>,
        conn_id: Uuid,
    ) -> Result<(), ()> {
        let id = frame.get_header("id").map(|s| s.to_string());
        if let Some(sub_id) = id {
            self.router.unsubscribe(conn_id, &sub_id).await;
        } else {
            let _ = send_command_frame(
                write_cmd_tx,
                create_error_frame("Missing UNSUBSCRIBE id"),
                true,
            )
            .await;
            return Err(());
        }
        Ok(())
    }

    async fn handle_send_cmd(
        &self,
        frame: &StompFrame<'_>,
        write_cmd_tx: &mpsc::Sender<WriteCommand>,
    ) -> Result<(), ()> {
        let destination = frame.get_header("destination").map(|s| s.to_string());
        if let Some(dest) = destination {
            let mut message_frame = StompFrame {
                command: Cow::Borrowed("MESSAGE"),
                headers: frame
                    .headers
                    .iter()
                    .filter(|(k, _)| k != "receipt" && k != "ack")
                    .cloned()
                    .collect(),
                body: frame.body.as_ref().map(|b| Cow::Owned(b.as_ref().to_vec())),
            };
            let message_id = Uuid::new_v4().to_string();
            message_frame
                .headers
                .push(("message-id".to_string(), message_id));
            if let Err(message) = self.router.publish(&dest, message_frame).await {
                let _ = send_command_frame(write_cmd_tx, create_error_frame(message), true).await;
                return Err(());
            }
        } else {
            let _ = send_command_frame(
                write_cmd_tx,
                create_error_frame("Missing destination header"),
                true,
            )
            .await;
            return Err(());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct StompConnectionService {
    server: StompServer,
}

impl StompConnectionService {
    pub fn new(server: StompServer) -> Self {
        Self { server }
    }
}

impl<S> Service<S> for StompConnectionService
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Response = ();
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, stream: S) -> Self::Future {
        let server = self.server.clone();
        Box::pin(async move {
            server.handle_connection(stream).await;
            Ok(())
        })
    }
}

async fn send_command_frame(
    write_cmd_tx: &mpsc::Sender<WriteCommand>,
    frame: StompFrame<'static>,
    wait_for_completion: bool,
) -> WriteResult {
    if wait_for_completion {
        let (completion_tx, completion_rx) = oneshot::channel();
        write_cmd_tx
            .send(WriteCommand::Frame(frame, Some(completion_tx)))
            .await
            .map_err(|_| disconnected_io_error())?;
        completion_rx.await.map_err(|_| disconnected_io_error())?
    } else {
        write_cmd_tx
            .send(WriteCommand::Frame(frame, None))
            .await
            .map_err(|_| disconnected_io_error())
    }
}

fn disconnected_io_error() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "connection writer stopped")
}

async fn send_error<W>(
    writer: &mut FramedWrite<W, StompCodec>,
    message: &str,
) -> Result<(), std::io::Error>
where
    W: AsyncWrite + Unpin + Send + 'static,
{
    writer.send(create_error_frame(message)).await
}

fn create_error_frame(message: &str) -> StompFrame<'static> {
    StompFrame {
        command: Cow::Borrowed("ERROR"),
        headers: vec![("message".to_string(), message.to_string())],
        body: None,
    }
}

fn create_receipt_frame(receipt_id: String) -> StompFrame<'static> {
    StompFrame {
        command: Cow::Borrowed("RECEIPT"),
        headers: vec![("receipt-id".to_string(), receipt_id)],
        body: None,
    }
}

fn serialize_server_frame(item: &ServerFrame, version: StompVersion, dst: &mut bytes::BytesMut) {
    match item {
        ServerFrame::Owned(frame) => {
            frame.serialize_to_buf(version, dst);
        }
        ServerFrame::Borrowed(frame, subscription, ack) => {
            serialize_shared_frame(frame, subscription.as_deref(), ack.as_deref(), version, dst);
        }
    }
}

fn serialize_shared_frame<B: bytes::BufMut>(
    frame: &StompFrame<'static>,
    subscription: Option<&str>,
    ack: Option<&str>,
    version: StompVersion,
    buf: &mut B,
) {
    let escape = !stompoxide_codec::is_control_frame(&frame.command);

    buf.put_slice(frame.command.as_bytes());
    buf.put_u8(b'\n');

    frame
        .headers
        .iter()
        .filter(|(key, _)| key != "content-length" && key != "subscription" && key != "ack")
        .for_each(|(key, val)| {
            stompoxide_codec::write_escaped_header(buf, key, val, escape, version);
        });

    if let Some(sub_id) = subscription {
        stompoxide_codec::write_escaped_header(buf, "subscription", sub_id, escape, version);
    }

    if let Some(ack_id) = ack {
        stompoxide_codec::write_escaped_header(buf, "ack", ack_id, escape, version);
    }

    if let Some(body) = &frame.body {
        buf.put_slice(&stompoxide_codec::get_content_length_header(body));
        buf.put_u8(b'\n');
        buf.put_slice(body);
    } else {
        buf.put_u8(b'\n');
    }
    buf.put_u8(b'\x00');
}

#[cfg(test)]
mod tests;
