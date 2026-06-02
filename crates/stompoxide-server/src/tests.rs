use futures_util::{SinkExt, StreamExt};
use stompoxide_client::{AckMode, ClientConfig, SendRequest, StompClient, SubscribeRequest};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tower::ServiceExt;

use super::*;

#[tokio::test]
async fn test_stompoxide_server_pub_sub() {
    let server = StompServer::new();
    let addr = "127.0.0.1:0";
    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let s = server_clone.clone();
                tokio::spawn(async move {
                    s.handle_connection(socket).await;
                });
            }
        }
    });

    // Start client 1 (subscriber)
    let stream1 = TcpStream::connect(local_addr).await.unwrap();
    let (client1, _h1) = StompClient::connect(stream1, ClientConfig::default())
        .await
        .unwrap();
    let (_, subscriber1) = client1.split();

    // Subscribe to wildcard destination `/topic/*`
    let mut sub1 = subscriber1
        .subscribe(SubscribeRequest::new("/topic/*"))
        .await
        .unwrap();

    // Start client 2 (publisher)
    let stream2 = TcpStream::connect(local_addr).await.unwrap();
    let (client2, _h2) = StompClient::connect(stream2, ClientConfig::default())
        .await
        .unwrap();
    let (sender2, _) = client2.split();

    // Send a message to `/topic/hello`
    sender2
        .send(SendRequest::new("/topic/hello", b"hello pubsub".to_vec()))
        .await
        .unwrap();

    // Wait and verify message received on client 1 subscription stream
    let msg = sub1.next().await.unwrap();
    assert_eq!(msg.command, "MESSAGE");
    assert_eq!(msg.body.unwrap().as_ref(), b"hello pubsub");
}

#[tokio::test]
async fn test_stompoxide_connection_service_handles_stream() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let service = server.connection_service();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            service.oneshot(socket).await.unwrap();
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let (client, _handle) = StompClient::connect(stream, ClientConfig::default())
        .await
        .unwrap();
    let (sender, subscriber) = client.split();
    let mut subscription = subscriber
        .subscribe(SubscribeRequest::new("/topic/service"))
        .await
        .unwrap();

    sender
        .send(SendRequest::new("/topic/service", "service"))
        .await
        .unwrap();

    let msg = subscription.next().await.unwrap();
    assert_eq!(msg.body.unwrap().as_ref(), b"service");
}

#[tokio::test]
async fn test_stompoxide_server_topic_broadcasts_to_all_matching_subscribers() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let s = server_clone.clone();
                tokio::spawn(async move {
                    s.handle_connection(socket).await;
                });
            }
        }
    });

    let stream1 = TcpStream::connect(local_addr).await.unwrap();
    let (client1, _h1) = StompClient::connect(stream1, ClientConfig::default())
        .await
        .unwrap();
    let (_, subscriber1) = client1.split();
    let mut sub1 = subscriber1
        .subscribe(SubscribeRequest::new("/topic/events"))
        .await
        .unwrap();

    let stream2 = TcpStream::connect(local_addr).await.unwrap();
    let (client2, _h2) = StompClient::connect(stream2, ClientConfig::default())
        .await
        .unwrap();
    let (_, subscriber2) = client2.split();
    let mut sub2 = subscriber2
        .subscribe(SubscribeRequest::new("/topic/*"))
        .await
        .unwrap();

    let stream3 = TcpStream::connect(local_addr).await.unwrap();
    let (client3, _h3) = StompClient::connect(stream3, ClientConfig::default())
        .await
        .unwrap();
    let (sender3, _) = client3.split();

    sender3
        .send(SendRequest::new("/topic/events", b"broadcast".to_vec()))
        .await
        .unwrap();

    let msg1 = sub1.next().await.unwrap();
    let msg2 = sub2.next().await.unwrap();
    assert_eq!(msg1.body.unwrap().as_ref(), b"broadcast");
    assert_eq!(msg2.body.unwrap().as_ref(), b"broadcast");
}

#[tokio::test]
async fn test_stompoxide_server_queue_round_robins_to_one_subscriber() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let s = server_clone.clone();
                tokio::spawn(async move {
                    s.handle_connection(socket).await;
                });
            }
        }
    });

    let stream1 = TcpStream::connect(local_addr).await.unwrap();
    let (client1, _h1) = StompClient::connect(stream1, ClientConfig::default())
        .await
        .unwrap();
    let (_, subscriber1) = client1.split();
    let mut sub1 = subscriber1
        .subscribe(SubscribeRequest::new("/queue/jobs"))
        .await
        .unwrap();

    let stream2 = TcpStream::connect(local_addr).await.unwrap();
    let (client2, _h2) = StompClient::connect(stream2, ClientConfig::default())
        .await
        .unwrap();
    let (_, subscriber2) = client2.split();
    let mut sub2 = subscriber2
        .subscribe(SubscribeRequest::new("/queue/jobs"))
        .await
        .unwrap();

    let stream3 = TcpStream::connect(local_addr).await.unwrap();
    let (client3, _h3) = StompClient::connect(stream3, ClientConfig::default())
        .await
        .unwrap();
    let (sender3, _) = client3.split();

    sender3
        .send(SendRequest::new("/queue/jobs", b"first".to_vec()))
        .await
        .unwrap();
    sender3
        .send(SendRequest::new("/queue/jobs", b"second".to_vec()))
        .await
        .unwrap();

    let msg1 = sub1.next().await.unwrap();
    let msg2 = sub2.next().await.unwrap();
    assert_eq!(msg1.body.unwrap().as_ref(), b"first");
    assert_eq!(msg2.body.unwrap().as_ref(), b"second");
}

#[tokio::test]
async fn test_stompoxide_server_queue_without_subscribers_drops_message() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let (client, _handle) = StompClient::connect(stream, ClientConfig::default())
        .await
        .unwrap();

    client
        .send(SendRequest::new("/queue/no-consumers", b"drop".to_vec()))
        .await
        .unwrap();
    client
        .send(SendRequest::new(
            "/queue/no-consumers",
            b"still connected".to_vec(),
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_stompoxide_server_rejects_unknown_send_destination() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.2".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();
    assert_eq!(framed.next().await.unwrap().unwrap().command, "CONNECTED");

    framed
        .send(StompFrame {
            command: "SEND".into(),
            headers: vec![("destination".to_string(), "/unknown/test".to_string())],
            body: Some(b"bad".as_slice().into()),
        })
        .await
        .unwrap();

    let error = framed.next().await.unwrap().unwrap();
    assert_eq!(error.command, "ERROR");
    assert_eq!(
        error
            .headers
            .iter()
            .find(|(k, _)| k == "message")
            .unwrap()
            .1,
        "Unknown destination"
    );
    assert!(framed.next().await.is_none());
}

#[tokio::test]
async fn test_stompoxide_server_rejects_unknown_subscribe_destination() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.2".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();
    assert_eq!(framed.next().await.unwrap().unwrap().command, "CONNECTED");

    framed
        .send(StompFrame {
            command: "SUBSCRIBE".into(),
            headers: vec![
                ("id".to_string(), "sub-1".to_string()),
                ("destination".to_string(), "/unknown/test".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let error = framed.next().await.unwrap().unwrap();
    assert_eq!(error.command, "ERROR");
    assert_eq!(
        error
            .headers
            .iter()
            .find(|(k, _)| k == "message")
            .unwrap()
            .1,
        "Unknown destination"
    );
    assert!(framed.next().await.is_none());
}

#[tokio::test]
async fn test_stompoxide_server_rejects_queue_wildcard_subscription() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.2".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();
    assert_eq!(framed.next().await.unwrap().unwrap().command, "CONNECTED");

    framed
        .send(StompFrame {
            command: "SUBSCRIBE".into(),
            headers: vec![
                ("id".to_string(), "sub-1".to_string()),
                ("destination".to_string(), "/queue/*".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let error = framed.next().await.unwrap().unwrap();
    assert_eq!(error.command, "ERROR");
    assert_eq!(
        error
            .headers
            .iter()
            .find(|(k, _)| k == "message")
            .unwrap()
            .1,
        "Queue subscriptions must use an exact destination"
    );
    assert!(framed.next().await.is_none());
}

#[tokio::test]
async fn test_stompoxide_server_version_negotiation_failure() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let mut stream = TcpStream::connect(local_addr).await.unwrap();
    // Send CONNECT with incompatible version 2.0
    stream
        .write_all(b"CONNECT\naccept-version:2.0\n\n\0")
        .await
        .unwrap();
    stream.flush().await.unwrap();

    let mut reader = FramedRead::new(stream, StompCodec::default());
    let error_frame = reader.next().await.unwrap().unwrap();
    assert_eq!(error_frame.command, "ERROR");
    let has_version_header = error_frame
        .headers
        .iter()
        .any(|(k, v)| k == "version" && v == "1.2,1.1,1.0");
    assert!(has_version_header);
}

#[tokio::test]
async fn test_stompoxide_server_version_negotiation_success() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let mut stream = TcpStream::connect(local_addr).await.unwrap();
    // Send CONNECT with version 1.2.
    stream
        .write_all(b"CONNECT\naccept-version:1.2\nhost:localhost\n\n\0")
        .await
        .unwrap();
    stream.flush().await.unwrap();

    let mut reader = FramedRead::new(stream, StompCodec::default());
    let connected_frame = reader.next().await.unwrap().unwrap();
    assert_eq!(connected_frame.command, "CONNECTED");
    assert_eq!(
        connected_frame
            .headers
            .iter()
            .find(|(k, _)| k == "version")
            .unwrap()
            .1,
        "1.2"
    );
}

#[tokio::test]
async fn test_stompoxide_server_version_negotiation_success_1_1() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let mut stream = TcpStream::connect(local_addr).await.unwrap();
    stream
        .write_all(b"CONNECT\naccept-version:1.0,1.1\nhost:localhost\n\n\0")
        .await
        .unwrap();
    stream.flush().await.unwrap();

    let mut reader = FramedRead::new(stream, StompCodec::default());
    let connected_frame = reader.next().await.unwrap().unwrap();
    assert_eq!(connected_frame.command, "CONNECTED");
    assert_eq!(
        connected_frame
            .headers
            .iter()
            .find(|(k, _)| k == "version")
            .unwrap()
            .1,
        "1.1"
    );
}

#[tokio::test]
async fn test_stompoxide_server_receipt() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    // Connect
    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.2".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let connected = framed.next().await.unwrap().unwrap();
    assert_eq!(connected.command, "CONNECTED");

    // Send message with receipt
    framed
        .send(StompFrame {
            command: "SEND".into(),
            headers: vec![
                ("destination".to_string(), "/topic/test".to_string()),
                ("receipt".to_string(), "receipt-789".to_string()),
            ],
            body: Some(b"hello".as_slice().into()),
        })
        .await
        .unwrap();

    let receipt = framed.next().await.unwrap().unwrap();
    assert_eq!(receipt.command, "RECEIPT");
    assert_eq!(
        receipt
            .headers
            .iter()
            .find(|(k, _)| k == "receipt-id")
            .unwrap()
            .1,
        "receipt-789"
    );
}

#[tokio::test]
async fn test_duplicate_headers() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    // Send CONNECT with duplicate accept-version headers: 1.2 first, then 1.0 (invalid).
    // If the server respected 1.0 (subsequent), it would reject with ERROR.
    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.2".to_string()),
                ("accept-version".to_string(), "1.0".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let connected = framed.next().await.unwrap().unwrap();
    assert_eq!(connected.command, "CONNECTED");

    // Send SEND with duplicate receipt headers: first "receipt-first", then "receipt-second".
    framed
        .send(StompFrame {
            command: "SEND".into(),
            headers: vec![
                ("destination".to_string(), "/topic/test".to_string()),
                ("receipt".to_string(), "receipt-first".to_string()),
                ("receipt".to_string(), "receipt-second".to_string()),
            ],
            body: Some(b"hello".as_slice().into()),
        })
        .await
        .unwrap();

    let receipt = framed.next().await.unwrap().unwrap();
    assert_eq!(receipt.command, "RECEIPT");
    assert_eq!(receipt.get_header("receipt-id"), Some("receipt-first"));
}

#[tokio::test]
async fn test_server_ack_nack_and_message_ack_header() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    // Connect
    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.2".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let connected = framed.next().await.unwrap().unwrap();
    assert_eq!(connected.command, "CONNECTED");

    // Subscribe with ack:client
    framed
        .send(StompFrame {
            command: "SUBSCRIBE".into(),
            headers: vec![
                ("id".to_string(), "sub-1".to_string()),
                ("destination".to_string(), "/topic/ack-test".to_string()),
                ("ack".to_string(), "client".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    // Send a message
    framed
        .send(StompFrame {
            command: "SEND".into(),
            headers: vec![("destination".to_string(), "/topic/ack-test".to_string())],
            body: Some(b"hello ack".as_slice().into()),
        })
        .await
        .unwrap();

    // Receive the message. It MUST contain an "ack" header.
    let message = framed.next().await.unwrap().unwrap();
    assert_eq!(message.command, "MESSAGE");
    let ack_header = message.get_header("ack").map(String::from);
    assert!(
        ack_header.is_some(),
        "MESSAGE frame is missing the mandatory 'ack' header"
    );
    let ack_id = ack_header.unwrap();

    // Send ACK back to the server with a receipt requested
    framed
        .send(StompFrame {
            command: "ACK".into(),
            headers: vec![
                ("id".to_string(), ack_id.clone()),
                ("receipt".to_string(), "receipt-ack".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    // The server should reply with a RECEIPT for the ACK frame
    let receipt = framed.next().await.unwrap().unwrap();
    assert_eq!(receipt.command, "RECEIPT");
    assert_eq!(receipt.get_header("receipt-id"), Some("receipt-ack"));

    // Send NACK to the server with a receipt requested
    framed
        .send(StompFrame {
            command: "NACK".into(),
            headers: vec![
                ("id".to_string(), ack_id),
                ("receipt".to_string(), "receipt-nack".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    // The server should reply with a RECEIPT for the NACK frame
    let receipt2 = framed.next().await.unwrap().unwrap();
    assert_eq!(receipt2.command, "RECEIPT");
    assert_eq!(receipt2.get_header("receipt-id"), Some("receipt-nack"));
}

#[tokio::test]
async fn test_server_message_no_ack_header_for_auto_ack() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    // Connect
    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.2".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let _connected = framed.next().await.unwrap().unwrap();

    // Subscribe (defaults to ack:auto)
    framed
        .send(StompFrame {
            command: "SUBSCRIBE".into(),
            headers: vec![
                ("id".to_string(), "sub-1".to_string()),
                (
                    "destination".to_string(),
                    "/topic/auto-ack-test".to_string(),
                ),
            ],
            body: None,
        })
        .await
        .unwrap();

    // Send a message
    framed
        .send(StompFrame {
            command: "SEND".into(),
            headers: vec![
                (
                    "destination".to_string(),
                    "/topic/auto-ack-test".to_string(),
                ),
                ("ack".to_string(), "sender-supplied-ack".to_string()),
            ],
            body: Some(b"hello auto".as_slice().into()),
        })
        .await
        .unwrap();

    // Receive the message. It MUST NOT contain an "ack" header.
    let message = framed.next().await.unwrap().unwrap();
    assert_eq!(message.command, "MESSAGE");
    assert!(
        message.get_header("ack").is_none(),
        "MESSAGE frame should not contain an 'ack' header for ack:auto subscriptions"
    );
}

#[tokio::test]
async fn test_stompoxide_transactions() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let s = server_clone.clone();
                tokio::spawn(async move {
                    s.handle_connection(socket).await;
                });
            }
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let (client, _handle) = StompClient::connect(stream, ClientConfig::default())
        .await
        .unwrap();
    let (sender, subscriber) = client.split();

    let mut subscription = subscriber
        .subscribe(SubscribeRequest::new("/topic/tx-test"))
        .await
        .unwrap();

    // 1. Begin transaction, send message, verify not received
    sender.begin("tx-1").await.unwrap();
    sender
        .send(SendRequest::new("/topic/tx-test", b"tx message 1".to_vec()).transaction("tx-1"))
        .await
        .unwrap();

    // Check with a timeout that subscription does not yield anything yet
    let check_no_msg = tokio::time::timeout(Duration::from_millis(100), subscription.next()).await;
    assert!(check_no_msg.is_err(), "Message was received before commit!");

    // 2. Abort transaction, verify not received
    sender.abort("tx-1").await.unwrap();
    let check_no_msg_after_abort =
        tokio::time::timeout(Duration::from_millis(100), subscription.next()).await;
    assert!(
        check_no_msg_after_abort.is_err(),
        "Message was received after abort!"
    );

    // 3. Begin new transaction, send message, commit, verify received
    sender.begin("tx-2").await.unwrap();
    sender
        .send(SendRequest::new("/topic/tx-test", b"tx message 2".to_vec()).transaction("tx-2"))
        .await
        .unwrap();

    sender.commit("tx-2").await.unwrap();

    let msg = tokio::time::timeout(Duration::from_millis(500), subscription.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg.body.unwrap().as_ref(), b"tx message 2");
}

#[tokio::test]
async fn test_stompoxide_ack_nack_redelivery() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                let s = server_clone.clone();
                tokio::spawn(async move {
                    s.handle_connection(socket).await;
                });
            }
        }
    });

    // Client 1 (Consumer) connects
    let stream1 = TcpStream::connect(local_addr).await.unwrap();
    let (client1, _h1) = StompClient::connect(stream1, ClientConfig::default())
        .await
        .unwrap();
    let (sender1, subscriber1) = client1.split();
    let mut sub1 = subscriber1
        .subscribe(SubscribeRequest::new("/queue/ack-nack-test").ack(AckMode::ClientIndividual))
        .await
        .unwrap();

    // Client 2 (Publisher) connects and sends a message
    let stream2 = TcpStream::connect(local_addr).await.unwrap();
    let (client2, _h2) = StompClient::connect(stream2, ClientConfig::default())
        .await
        .unwrap();
    let (sender2, _) = client2.split();
    sender2
        .send(SendRequest::new(
            "/queue/ack-nack-test",
            b"hello redelivery".to_vec(),
        ))
        .await
        .unwrap();

    // Client 1 receives the message
    let msg1 = tokio::time::timeout(Duration::from_millis(500), sub1.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg1.body.as_ref().unwrap().as_ref(), b"hello redelivery");
    let ack_id = msg1.get_header("ack").unwrap().to_string();

    // Client 1 sends NACK -> message should be redelivered
    sender1.nack(ack_id).await.unwrap();

    // Client 1 receives the redelivered message again
    let msg2 = tokio::time::timeout(Duration::from_millis(500), sub1.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg2.body.as_ref().unwrap().as_ref(), b"hello redelivery");

    // Client 3 connects and subscribes BEFORE Client 1 disconnects
    let stream3 = TcpStream::connect(local_addr).await.unwrap();
    let (client3, _h3) = StompClient::connect(stream3, ClientConfig::default())
        .await
        .unwrap();
    let (_, subscriber3) = client3.split();
    let mut sub3 = subscriber3
        .subscribe(SubscribeRequest::new("/queue/ack-nack-test").ack(AckMode::Auto))
        .await
        .unwrap();

    // Client 1 drops/disconnects (which should return un-ACKed messages to queue)
    drop(sub1);
    drop(sender1);
    drop(subscriber1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client 3 should receive the returned message
    let msg3 = tokio::time::timeout(Duration::from_millis(500), sub3.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(msg3.body.as_ref().unwrap().as_ref(), b"hello redelivery");
}

#[tokio::test]
async fn test_stompoxide_server_authentication_success() {
    let server = StompServer::new()
        .with_authenticator(|login, passcode| login == "admin" && passcode == "secret");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let config = ClientConfig {
        login: Some("admin".to_string()),
        passcode: Some("secret".to_string()),
        ..ClientConfig::default()
    };
    let client = StompClient::connect(stream, config).await;
    assert!(client.is_ok());
}

#[tokio::test]
async fn test_stompoxide_server_authentication_failure() {
    let server = StompServer::new()
        .with_authenticator(|login, passcode| login == "admin" && passcode == "secret");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let config = ClientConfig {
        login: Some("admin".to_string()),
        passcode: Some("wrong_secret".to_string()),
        ..ClientConfig::default()
    };
    let client = StompClient::connect(stream, config).await;
    assert!(client.is_err());
}

#[tokio::test]
async fn test_stomp_1_1_ack_nack_requires_subscription_regression() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    // Connect as 1.1
    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.1".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let connected = framed.next().await.unwrap().unwrap();
    assert_eq!(connected.command, "CONNECTED");

    // Send ACK missing subscription
    framed
        .send(StompFrame {
            command: "ACK".into(),
            headers: vec![("message-id".to_string(), "some-msg-id".to_string())],
            body: None,
        })
        .await
        .unwrap();

    let error_frame = framed.next().await.unwrap().unwrap();
    assert_eq!(error_frame.command, "ERROR");
    assert!(
        error_frame
            .get_header("message")
            .unwrap()
            .contains("Missing subscription header in ACK for STOMP 1.1")
    );
}

#[tokio::test]
async fn test_stomp_1_0_nack_rejected_regression() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    // Connect as 1.0 (empty version or no accept-version)
    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![],
            body: None,
        })
        .await
        .unwrap();

    let connected = framed.next().await.unwrap().unwrap();
    assert_eq!(connected.command, "CONNECTED");

    // Send NACK (1.0 doesn't support it)
    framed
        .send(StompFrame {
            command: "NACK".into(),
            headers: vec![("message-id".to_string(), "some-msg-id".to_string())],
            body: None,
        })
        .await
        .unwrap();

    let error_frame = framed.next().await.unwrap().unwrap();
    assert_eq!(error_frame.command, "ERROR");
    assert!(
        error_frame
            .get_header("message")
            .unwrap()
            .contains("STOMP 1.0 does not support NACK")
    );
}

#[tokio::test]
async fn test_client_stomp_1_1_enforces_subscription_regression() {
    use stompoxide_client::{ClientConfig, StompClient, StompError};

    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let config = ClientConfig {
        accept_versions: vec!["1.1".to_string()],
        ..ClientConfig::default()
    };
    let (client, _handle) = StompClient::connect(stream, config).await.unwrap();

    // Call raw ack (missing subscription) -> should fail locally
    let res = client.ack("some-msg-id").await;
    assert!(res.is_err());
    if let Err(StompError::Protocol(msg)) = res {
        assert!(msg.contains("STOMP 1.1 ACK requires a subscription header"));
    } else {
        panic!("expected protocol error");
    }

    // Call raw nack (missing subscription) -> should fail locally
    let res_nack = client.nack("some-msg-id").await;
    assert!(res_nack.is_err());
    if let Err(StompError::Protocol(msg)) = res_nack {
        assert!(msg.contains("STOMP 1.1 NACK requires a subscription header"));
    } else {
        panic!("expected protocol error");
    }
}

#[tokio::test]
async fn test_stomp_1_2_ack_requires_id_regression() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.2".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let connected = framed.next().await.unwrap().unwrap();
    assert_eq!(connected.command, "CONNECTED");

    framed
        .send(StompFrame {
            command: "ACK".into(),
            headers: vec![("message-id".to_string(), "some-msg-id".to_string())],
            body: None,
        })
        .await
        .unwrap();

    let error_frame = framed.next().await.unwrap().unwrap();
    assert_eq!(error_frame.command, "ERROR");
    assert!(
        error_frame
            .get_header("message")
            .unwrap()
            .contains("Missing id header in ACK")
    );
}

#[tokio::test]
async fn test_stomp_1_1_transaction_ack_requires_subscription_regression() {
    let server = StompServer::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_clone = server.clone();
    tokio::spawn(async move {
        if let Ok((socket, _)) = listener.accept().await {
            server_clone.handle_connection(socket).await;
        }
    });

    let stream = TcpStream::connect(local_addr).await.unwrap();
    let mut framed = Framed::new(stream, StompCodec::default());

    framed
        .send(StompFrame {
            command: "CONNECT".into(),
            headers: vec![
                ("accept-version".to_string(), "1.1".to_string()),
                ("host".to_string(), "localhost".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let connected = framed.next().await.unwrap().unwrap();
    assert_eq!(connected.command, "CONNECTED");

    framed
        .send(StompFrame {
            command: "BEGIN".into(),
            headers: vec![("transaction".to_string(), "tx-1".to_string())],
            body: None,
        })
        .await
        .unwrap();

    framed
        .send(StompFrame {
            command: "ACK".into(),
            headers: vec![
                ("message-id".to_string(), "some-msg-id".to_string()),
                ("transaction".to_string(), "tx-1".to_string()),
            ],
            body: None,
        })
        .await
        .unwrap();

    let error_frame = framed.next().await.unwrap().unwrap();
    assert_eq!(error_frame.command, "ERROR");
    assert!(
        error_frame
            .get_header("message")
            .unwrap()
            .contains("Missing subscription header in ACK for STOMP 1.1")
    );
}
