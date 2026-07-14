//! WebSocket transport for MCP
//!
//! rmcp does not provide WebSocket transport (their ws.rs is an empty stub),
//! so we implement it ourselves using tokio-tungstenite.

use std::marker::PhantomData;

use futures::{Sink, Stream};
use rmcp::service::{RxJsonRpcMessage, ServiceRole, TxJsonRpcMessage};
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest, http::HeaderMap};

pin_project_lite::pin_project! {
    /// WebSocket transport wrapper that implements Stream + Sink for rmcp
    ///
    /// This adapts a tokio-tungstenite WebSocket stream to work with rmcp's
    /// transport system by:
    /// - Converting outgoing JSON-RPC messages to WebSocket text frames
    /// - Parsing incoming WebSocket text frames as JSON-RPC messages
    pub struct WebSocketTransport<R, S, E> {
        #[pin]
        stream: S,
        marker: PhantomData<(fn() -> E, fn() -> R)>
    }
}

impl<R, S, E> WebSocketTransport<R, S, E> {
    /// Create a new WebSocket transport from any compatible stream
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            marker: PhantomData,
        }
    }
}

impl<R, S, E> Stream for WebSocketTransport<R, S, E>
where
    S: Stream<Item = Result<tungstenite::Message, E>>,
    R: ServiceRole,
    E: std::error::Error,
{
    type Item = RxJsonRpcMessage<R>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().project();
        match this.stream.poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(message))) => {
                // Only process text messages, skip others (ping, pong, binary, close)
                let json = match message {
                    tungstenite::Message::Text(text) => text,
                    _ => return self.poll_next(cx),
                };
                // Parse JSON-RPC message
                match serde_json::from_str::<RxJsonRpcMessage<R>>(&json) {
                    Ok(message) => std::task::Poll::Ready(Some(message)),
                    Err(e) => {
                        tracing::warn!(error = %e, "Failed to parse JSON-RPC message from WebSocket");
                        self.poll_next(cx)
                    }
                }
            }
            std::task::Poll::Ready(Some(Err(e))) => {
                tracing::warn!(error = %e, "WebSocket error");
                self.poll_next(cx)
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<R, S, E> Sink<TxJsonRpcMessage<R>> for WebSocketTransport<R, S, E>
where
    S: Sink<tungstenite::Message, Error = E>,
    R: ServiceRole,
{
    type Error = E;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().stream.poll_ready(cx)
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: TxJsonRpcMessage<R>,
    ) -> Result<(), Self::Error> {
        let json = serde_json::to_string(&item).expect("JSON-RPC message should serialize to JSON");
        let message = tungstenite::Message::Text(json.into());
        self.project().stream.start_send(message)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.project().stream.poll_close(cx)
    }
}

/// Connect to a WebSocket MCP server
///
/// Returns a WebSocketTransport that implements Stream + Sink and can be
/// passed to rmcp's `serve()` method.
pub async fn connect(
    url: &str,
) -> Result<
    WebSocketTransport<
        rmcp::RoleClient,
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tungstenite::Error,
    >,
    crate::error::Error,
> {
    connect_with_headers(url, HeaderMap::new()).await
}

/// Connect to a WebSocket MCP server with validated handshake headers.
pub async fn connect_with_headers(
    url: &str,
    headers: HeaderMap,
) -> Result<
    WebSocketTransport<
        rmcp::RoleClient,
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        tungstenite::Error,
    >,
    crate::error::Error,
> {
    let mut request = url
        .into_client_request()
        .map_err(|e| crate::error::Error::Transport(format!("Invalid WebSocket URL: {e}")))?;
    request.headers_mut().extend(headers);

    let (stream, response) = tokio_tungstenite::connect_async(request)
        .await
        .map_err(|e| {
            crate::error::Error::Transport(format!("WebSocket connection failed: {}", e))
        })?;

    if response.status() != tungstenite::http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(crate::error::Error::Transport(format!(
            "WebSocket upgrade failed with status: {}",
            response.status()
        )));
    }

    Ok(WebSocketTransport::new(stream))
}
