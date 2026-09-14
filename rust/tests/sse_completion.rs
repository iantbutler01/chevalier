use std::time::Duration;

use chevalier::utils::parse_sse_stream;
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

#[tokio::test]
async fn done_ends_the_stream_without_waiting_for_http_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (release, wait) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        socket.read(&mut request).await.unwrap();
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let body = "data: {\"usage\":{\"completion_tokens\":7}}\n\ndata: [DONE]\n\n";
        socket
            .write_all(format!("{:x}\r\n{}\r\n", body.len(), body).as_bytes())
            .await
            .unwrap();
        socket.flush().await.unwrap();
        // An upstream may keep the HTTP body open after its terminal SSE event.
        let _ = wait.await;
    });
    let response = reqwest::get(format!("http://{address}")).await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(1),
        parse_sse_stream(response).collect::<Vec<_>>(),
    )
    .await;
    let _ = release.send(());
    server.await.unwrap();
    let events = result.expect("[DONE] must finish the stream before the connection closes");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].as_ref().unwrap()["usage"]["completion_tokens"], 7);
}
