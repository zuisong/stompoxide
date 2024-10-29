use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use super::*;

#[test]
fn send_request_headers_replace_existing_headers_and_receipt_upserts() {
    let frame = send_request_to_frame(
        SendRequest::new("/queue/jobs", "hello")
            .receipt("receipt-1")
            .headers(vec![("content-type".to_string(), "text/plain".to_string())])
            .receipt("receipt-2"),
    );

    assert_eq!(frame.command, "SEND");
    assert_eq!(
        frame.headers,
        vec![
            ("content-type".to_string(), "text/plain".to_string()),
            ("receipt".to_string(), "receipt-2".to_string()),
            ("destination".to_string(), "/queue/jobs".to_string()),
        ]
    );
}

#[test]
fn subscribe_request_defaults_to_auto_ack() {
    let frame = subscribe_request_to_frame(SubscribeRequest::new("/queue/jobs"), "sub-1".into());

    assert_eq!(frame.command, "SUBSCRIBE");
    assert!(
        frame
            .headers
            .contains(&("ack".to_string(), "auto".to_string()))
    );
}

#[test]
fn subscribe_request_headers_replace_existing_headers_and_ack_upserts() {
    let frame = subscribe_request_to_frame(
        SubscribeRequest::new("/queue/jobs")
            .ack(AckMode::Client)
            .headers(vec![("receipt".to_string(), "receipt-1".to_string())])
            .ack(AckMode::ClientIndividual),
        "sub-1".into(),
    );

    assert_eq!(
        frame.headers,
        vec![
            ("receipt".to_string(), "receipt-1".to_string()),
            ("ack".to_string(), "client-individual".to_string()),
            ("id".to_string(), "sub-1".to_string()),
            ("destination".to_string(), "/queue/jobs".to_string()),
        ]
    );
}

#[tokio::test]
async fn test_client_connect_send_subscribe() {
    // Start mock STOMP server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(socket, StompCodec::default());

        // Expect CONNECT frame
        let connect = framed.next().await.unwrap().unwrap();
        assert_eq!(connect.command, "CONNECT");

        // Send CONNECTED frame
        framed
            .send(StompFrame {
                command: Cow::Borrowed("CONNECTED"),
                headers: vec![
                    ("version".to_string(), "1.2".to_string()),
                    ("heart-beat".to_string(), "0,0".to_string()),
                ],
                body: None,
            })
            .await
            .unwrap();

        // Expect SUBSCRIBE frame
        let subscribe = framed.next().await.unwrap().unwrap();
        assert_eq!(subscribe.command, "SUBSCRIBE");
        let sub_id = subscribe
            .headers
            .iter()
            .find(|(k, _)| k == "id")
            .map(|(_, v)| v.clone())
            .unwrap();

        // Expect SEND frame
        let send = framed.next().await.unwrap().unwrap();
        assert_eq!(send.command, "SEND");
        assert_eq!(send.body.as_ref().unwrap().as_ref(), b"hello server");

        // Forward message back as a MESSAGE frame
        framed
            .send(StompFrame {
                command: Cow::Borrowed("MESSAGE"),
                headers: vec![
                    ("subscription".to_string(), sub_id),
                    ("message-id".to_string(), "msg-001".to_string()),
                    ("destination".to_string(), "/topic/test".to_string()),
                ],
                body: Some(Cow::Borrowed(b"hello client")),
            })
            .await
            .unwrap();
    });

    // Run STOMP client
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let config = ClientConfig {
        host: "localhost".to_string(),
        ..Default::default()
    };
    let (client, _handle) = StompClient::connect(stream, config).await.unwrap();
    let (sender, subscriber) = client.split();

    // Subscribe
    let mut stream = subscriber
        .subscribe(SubscribeRequest::new("/topic/test"))
        .await
        .unwrap();

    // Send
    sender
        .send(SendRequest::new("/topic/test", b"hello server".to_vec()))
        .await
        .unwrap();

    // Wait and verify message received from subscription
    let msg = stream.next().await.unwrap();
    assert_eq!(msg.command, "MESSAGE");
    assert_eq!(msg.body.unwrap().as_ref(), b"hello client");

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_real_activemq_server() {
    // Try to connect to a local ActiveMQ instance on port 61613 (standard STOMP port).
    // If ActiveMQ is not running (e.g. Docker container not started), we skip the test gracefully.
    let addr = "127.0.0.1:61613";
    let stream = match tokio::net::TcpStream::connect(addr).await {
        Ok(s) => s,
        Err(_) => {
            println!(
                "Skipping real ActiveMQ test: ActiveMQ is not running on {}",
                addr
            );
            println!("To run the test, start ActiveMQ with the following command:");
            println!(
                "docker run --detach --name activemq-artemis  --network=host  --rm apache/activemq-artemis:latest-alpine"
            );
            return;
        }
    };

    println!("Running E2E test against local ActiveMQ on {}...", addr);

    let config = ClientConfig {
        host: "localhost".to_string(),
        login: Some("artemis".to_string()),
        passcode: Some("artemis".to_string()),
        heartbeat_cx: 5000,
        heartbeat_cy: 5000,
        accept_versions: vec!["1.0".to_string(), "1.1".to_string(), "1.2".to_string()],
    };

    let (client, _handle) = StompClient::connect(stream, config).await.unwrap();

    // Subscribe to a test topic
    let mut subscription = client
        .subscribe(SubscribeRequest::new("/topic/docker-test"))
        .await
        .unwrap();

    // Send a message
    let test_body = b"hello activemq via docker".to_vec();
    client
        .send(SendRequest::new("/topic/docker-test", test_body.clone()))
        .await
        .unwrap();

    // Receive the message
    let msg = tokio::time::timeout(Duration::from_secs(5), subscription.next())
        .await
        .expect("Timeout waiting for message from ActiveMQ")
        .expect("Subscription stream ended unexpectedly");

    assert_eq!(msg.command, "MESSAGE");
    assert_eq!(msg.body.unwrap().as_ref(), test_body.as_slice());
}

#[tokio::test]
async fn test_client_heartbeat_timeout() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        if let Ok((mut socket, _)) = listener.accept().await {
            // Send CONNECTED negotiating a very short heartbeat: server will send heartbeats every 100ms
            let connected_frame = b"CONNECTED\nversion:1.2\nheart-beat:100,100\n\n\0";
            socket.write_all(connected_frame).await.unwrap();
            socket.flush().await.unwrap();

            // Keep the socket open but sleep forever, sending no data/heartbeats
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    let stream = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    // Client config: wants to send every 0ms, expects every 100ms.
    let config = ClientConfig {
        host: "localhost".to_string(),
        login: None,
        passcode: None,
        heartbeat_cx: 0,
        heartbeat_cy: 100,
        accept_versions: vec!["1.0".to_string(), "1.1".to_string(), "1.2".to_string()],
    };

    let (_client, handle) = StompClient::connect(stream, config).await.unwrap();

    // The background connection loop should exit due to heartbeat timeout within 150-200ms
    let start = std::time::Instant::now();
    let res = tokio::time::timeout(Duration::from_millis(1000), handle).await;

    assert!(
        res.is_ok(),
        "The connection loop did not exit within timeout"
    );
    let join_res = res.unwrap();
    let loop_res = join_res.unwrap();
    assert!(
        matches!(loop_res, Err(StompError::Protocol(ref msg)) if msg.contains("Heartbeat timeout")),
        "Expected heartbeat timeout error, got {:?}",
        loop_res
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(150),
        "Exited too early: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_client_send_ack_nack() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let (frame_tx, mut frame_rx) = mpsc::channel(10);

    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            let mut framed = Framed::new(socket, StompCodec::default());
            // Read CONNECT
            let connect = framed.next().await.unwrap().unwrap();
            assert_eq!(connect.command, "CONNECT");

            // Send CONNECTED
            framed
                .send(StompFrame {
                    command: "CONNECTED".into(),
                    headers: vec![("version".to_string(), "1.2".to_string())],
                    body: None,
                })
                .await
                .unwrap();

            // Read ACK
            let ack = framed.next().await.unwrap().unwrap();
            frame_tx.send(ack).await.unwrap();

            // Read NACK
            let nack = framed.next().await.unwrap().unwrap();
            frame_tx.send(nack).await.unwrap();
        }
    });

    let stream = tokio::net::TcpStream::connect(local_addr).await.unwrap();
    let (client, _handle) = StompClient::connect(stream, ClientConfig::default())
        .await
        .unwrap();

    // Send ACK
    client.ack("msg-1").await.unwrap();

    // Send NACK
    client.nack("msg-2").await.unwrap();

    // Verify ACK frame received by the mock server
    let ack_received = frame_rx.recv().await.unwrap();
    assert_eq!(ack_received.command, "ACK");
    assert_eq!(ack_received.get_header("id"), Some("msg-1"));

    // Verify NACK frame received by the mock server
    let nack_received = frame_rx.recv().await.unwrap();
    assert_eq!(nack_received.command, "NACK");
    assert_eq!(nack_received.get_header("id"), Some("msg-2"));
}
