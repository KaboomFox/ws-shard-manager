use crate::config::{BackoffConfig, ConnectionConfig, HealthConfig};
use crate::error::Error;
use crate::handler::{ConnectionState, SequenceRecovery, WebSocketHandler};
use crate::health::HealthMonitor;
use crate::metrics::Metrics;
use futures_util::{SinkExt, StreamExt};
use http::{HeaderName, HeaderValue};
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{
    client_async_tls_with_config, tungstenite::client::IntoClientRequest, tungstenite::Message,
    Connector, MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, info, trace, warn};
use url::Url;

/// Commands that can be sent to a connection
#[derive(Debug)]
pub enum ConnectionCommand {
    /// Send a message
    Send(Message),
    /// Gracefully close the connection
    Close,
    /// Force reconnection
    Reconnect,
}

/// Sanitize a proxy URL for logging by removing credentials.
/// Returns the URL with username/password replaced with "***" if present.
fn sanitize_proxy_url(proxy: &str) -> String {
    match Url::parse(proxy) {
        Ok(mut url) => {
            if !url.username().is_empty() || url.password().is_some() {
                // Replace credentials with placeholder
                let _ = url.set_username("***");
                let _ = url.set_password(Some("***"));
            }
            url.to_string()
        }
        Err(_) => "[invalid-url]".to_string(),
    }
}

/// Manages a single WebSocket connection with auto-reconnection
pub struct Connection<H: WebSocketHandler> {
    shard_id: usize,
    handler: Arc<H>,
    config: ConnectionConfig,
    backoff: BackoffConfig,
    health_config: HealthConfig,
    metrics: Arc<Metrics>,
    command_rx: mpsc::Receiver<ConnectionCommand>,
    /// Optional channel to signal when connection is ready
    ready_tx: Option<oneshot::Sender<()>>,
    /// Explicit subscriptions for this shard (used during hot switchover)
    initial_subscriptions: Option<Vec<H::Subscription>>,
    /// Optional channel to request hot switchover from manager
    switchover_tx: Option<mpsc::Sender<usize>>,
}

impl<H: WebSocketHandler> Connection<H> {
    /// Create a new connection with a switchover request channel
    #[allow(clippy::too_many_arguments)]
    pub fn with_switchover_channel(
        shard_id: usize,
        handler: Arc<H>,
        config: ConnectionConfig,
        backoff: BackoffConfig,
        health_config: HealthConfig,
        metrics: Arc<Metrics>,
        command_rx: mpsc::Receiver<ConnectionCommand>,
        switchover_tx: mpsc::Sender<usize>,
    ) -> Self {
        Self {
            shard_id,
            handler,
            config,
            backoff,
            health_config,
            metrics,
            command_rx,
            ready_tx: None,
            initial_subscriptions: None,
            switchover_tx: Some(switchover_tx),
        }
    }

    /// Create a new connection with a ready signal for hot switchover
    #[allow(clippy::too_many_arguments)]
    pub fn with_ready_signal(
        shard_id: usize,
        handler: Arc<H>,
        config: ConnectionConfig,
        backoff: BackoffConfig,
        health_config: HealthConfig,
        metrics: Arc<Metrics>,
        command_rx: mpsc::Receiver<ConnectionCommand>,
        ready_tx: oneshot::Sender<()>,
        subscriptions: Vec<H::Subscription>,
        switchover_tx: mpsc::Sender<usize>,
    ) -> Self {
        Self {
            shard_id,
            handler,
            config,
            backoff,
            health_config,
            metrics,
            command_rx,
            ready_tx: Some(ready_tx),
            initial_subscriptions: Some(subscriptions),
            switchover_tx: Some(switchover_tx),
        }
    }

    /// Run the connection loop (reconnects on failure)
    pub async fn run(mut self) -> Result<(), Error> {
        let mut reconnect_attempt = 0u32;
        let mut is_first_connect = true;
        let mut consecutive_failures = 0u32;
        let mut circuit_breaker_tripped_at: Option<Instant> = None;

        loop {
            // Check circuit breaker
            if let Some(tripped_at) = circuit_breaker_tripped_at {
                if tripped_at.elapsed() < self.config.circuit_breaker_reset_timeout {
                    // Circuit breaker is open - wait for reset timeout
                    let remaining =
                        self.config.circuit_breaker_reset_timeout - tripped_at.elapsed();
                    warn!(
                        "[SHARD-{}] Circuit breaker open, waiting {:?} before retry",
                        self.shard_id, remaining
                    );
                    tokio::time::sleep(remaining).await;
                }
                // Reset circuit breaker after timeout
                circuit_breaker_tripped_at = None;
                consecutive_failures = 0;
            }

            // Get actual subscription count for this shard from metrics
            let subscription_count = self.metrics.shard_subscription_count(self.shard_id);

            let state = ConnectionState {
                shard_id: self.shard_id,
                subscription_count,
                is_reconnect: !is_first_connect,
                reconnect_attempt,
            };

            // Calculate backoff delay (skip on first connect)
            if !is_first_connect {
                let delay = self.backoff.delay_for_attempt(reconnect_attempt);
                debug!(
                    "[SHARD-{}] Reconnecting in {:?} (attempt {})",
                    self.shard_id,
                    delay,
                    reconnect_attempt + 1
                );
                tokio::time::sleep(delay).await;
            }

            match self.connect_and_run(&state).await {
                Ok(should_stop) => {
                    if should_stop {
                        info!("[SHARD-{}] Connection closed gracefully", self.shard_id);
                        return Ok(());
                    }
                    // Clean disconnect, reset counters
                    reconnect_attempt = 0;
                    consecutive_failures = 0;
                }
                Err(e) => {
                    self.metrics.record_error();
                    consecutive_failures += 1;
                    // Log at debug level since hot switchover handles these gracefully
                    debug!(
                        "[SHARD-{}] Connection error: {} (attempt {}, consecutive failures: {})",
                        self.shard_id,
                        e,
                        reconnect_attempt + 1,
                        consecutive_failures
                    );

                    // Check if we should trip the circuit breaker
                    if consecutive_failures >= self.config.circuit_breaker_threshold {
                        error!(
                            "[SHARD-{}] Circuit breaker tripped after {} consecutive failures",
                            self.shard_id, consecutive_failures
                        );
                        self.metrics.record_circuit_breaker_trip();
                        circuit_breaker_tripped_at = Some(Instant::now());
                        // Don't count against max_connect_attempts when circuit breaker is handling it
                        continue;
                    }

                    // Ask handler if we should retry (with panic protection)
                    let should_retry = self.call_on_error_safe(&e, &state).await;
                    if !should_retry {
                        error!("[SHARD-{}] Handler declined reconnection", self.shard_id);
                        return Err(e);
                    }

                    reconnect_attempt += 1;
                    if reconnect_attempt >= self.config.max_connect_attempts {
                        error!(
                            "[SHARD-{}] Max reconnection attempts ({}) reached",
                            self.shard_id, self.config.max_connect_attempts
                        );
                        return Err(Error::ConnectionFailed {
                            attempts: reconnect_attempt,
                            last_error: e.to_string(),
                        });
                    }
                }
            }

            is_first_connect = false;
            self.metrics.record_reconnection();
        }
    }

    /// Call handler.on_error with panic protection
    ///
    /// If the handler panics, we log the error and return true (retry).
    async fn call_on_error_safe(&self, error: &Error, state: &ConnectionState) -> bool {
        let handler = self.handler.clone();
        let error_kind = error.kind();
        let error_msg = error.to_string();
        let state_clone = state.clone();
        let shard_id = self.shard_id;

        let result = tokio::task::spawn(async move {
            let fut = AssertUnwindSafe(handler.on_error(error_kind, &error_msg, &state_clone));
            fut.await
        })
        .await;

        match result {
            Ok(should_retry) => should_retry,
            Err(e) => {
                if e.is_panic() {
                    error!(
                        "[SHARD-{}] Handler.on_error panicked! Defaulting to retry. Error: {:?}",
                        shard_id, e
                    );
                } else {
                    warn!("[SHARD-{}] Handler.on_error task failed: {:?}", shard_id, e);
                }
                true
            }
        }
    }

    /// Call handler.on_message, optionally with panic protection.
    ///
    /// In low-latency mode, handlers are called directly (panics crash the task).
    /// Otherwise, handlers are spawned in a separate task for panic isolation.
    #[inline]
    async fn call_on_message(&self, message: Message, state: &ConnectionState) {
        let start = Instant::now();
        if self.config.low_latency_mode {
            // Direct call - no spawn overhead (~1-5µs savings per message)
            self.handler.on_message(message, state).await;
            trace!(
                "[SHARD-{}] Handler processed message in {:?}",
                self.shard_id,
                start.elapsed()
            );
        } else {
            // Spawn for panic protection
            let handler = self.handler.clone();
            let state_clone = state.clone();
            let shard_id = self.shard_id;

            let result = tokio::task::spawn(async move {
                let start = Instant::now();
                let fut = AssertUnwindSafe(handler.on_message(message, &state_clone));
                fut.await;
                trace!(
                    "[SHARD-{}] Handler processed message in {:?}",
                    shard_id,
                    start.elapsed()
                );
            })
            .await;

            if let Err(e) = result {
                if e.is_panic() {
                    error!(
                        "[SHARD-{}] Handler.on_message panicked! Message dropped. Error: {:?}",
                        shard_id, e
                    );
                    self.metrics.record_error();
                } else {
                    warn!(
                        "[SHARD-{}] Handler.on_message task failed: {:?}",
                        shard_id, e
                    );
                }
            }
        }
    }

    /// Connect and run until disconnection
    /// Returns Ok(true) if should stop, Ok(false) if should reconnect
    async fn connect_and_run(&mut self, state: &ConnectionState) -> Result<bool, Error> {
        let url = self.handler.url(state).await;
        let headers = self.handler.connection_headers(state).await;
        let source_ip = self.config.source_ip_for_shard(self.shard_id);
        let proxy = self.config.proxy.as_deref();

        debug!(
            "[SHARD-{}] Connecting to {} (source_ip={:?}, proxy={:?}, headers={})",
            self.shard_id,
            url,
            source_ip,
            proxy.map(sanitize_proxy_url),
            headers.len()
        );

        // Connect with timeout, using source IP, proxy, and custom headers
        let ws_stream = match timeout(
            self.config.connect_timeout,
            connect_with_options(&url, source_ip, proxy, headers),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(Error::ConnectionFailed {
                    attempts: 0,
                    last_error: "Connection timeout".to_string(),
                })
            }
        };

        self.metrics.record_connection();
        self.metrics.update_shard(self.shard_id, |s| {
            s.is_connected = true;
            s.last_connected_at = Some(Instant::now());
            s.reconnect_attempt = state.reconnect_attempt;
        });

        let via = match (source_ip, proxy) {
            (Some(ip), Some(p)) => format!("{} via proxy {}", ip, p),
            (Some(ip), None) => ip.to_string(),
            (None, Some(p)) => format!("proxy {}", p),
            (None, None) => "default".to_string(),
        };
        info!("[SHARD-{}] Connected to {} ({})", self.shard_id, url, via);

        let (mut write, mut read) = ws_stream.split();

        // Reset sequence tracking state for new connection
        self.handler.reset_sequence_state(state);

        // Send initial messages from handler
        let init_messages = self.handler.on_connect(state).await;
        for msg in init_messages {
            write.send(msg).await?;
            self.metrics.record_message_sent();
        }

        // If we have explicit subscriptions (hot switchover), send subscription message
        if let Some(ref subs) = self.initial_subscriptions {
            if !subs.is_empty() {
                if let Some(msg) = self.handler.subscription_message(subs) {
                    write.send(msg).await?;
                    self.metrics.record_message_sent();
                    info!(
                        "[SHARD-{}] Sent {} subscriptions from hot switchover",
                        self.shard_id,
                        subs.len()
                    );
                }
            }
        }

        // Signal that connection is ready (for hot switchover)
        if let Some(ready_tx) = self.ready_tx.take() {
            let _ = ready_tx.send(());
            debug!("[SHARD-{}] Signaled connection ready", self.shard_id);
        }

        // Initialize health monitor
        let mut health = HealthMonitor::new(self.health_config.clone());

        // Proactive reconnect timer
        let proactive_reconnect_interval = self.config.proactive_reconnect_interval;
        let connection_start = Instant::now();

        // Pre-allocate ping data to avoid allocation in hot path
        let ping_data: Vec<u8> = format!("ping-{}", self.shard_id).into_bytes();

        let mut should_stop = false;
        let mut msg_count: u64 = 0;
        info!("[SHARD-{}] Entering message loop", self.shard_id);

        loop {
            // Calculate smart sleep duration based on health monitor state
            let next_health_check = health
                .time_until_next_ping()
                .min(health.time_until_data_timeout())
                .min(Duration::from_secs(1)); // Cap at 1 second for responsiveness

            tokio::select! {
                // Handle incoming messages
                msg = read.next() => {
                    match msg {
                        Some(Ok(message)) => {
                            self.metrics.record_message_received();
                            self.metrics.record_shard_message_received(self.shard_id);
                            // Log first few messages per connection for debugging
                            msg_count += 1;
                            if msg_count <= 5 {
                                info!("[SHARD-{}] WS message #{}: {} bytes, type: {:?}",
                                    self.shard_id, msg_count, message.len(),
                                    match &message {
                                        Message::Text(_) => "Text",
                                        Message::Binary(_) => "Binary",
                                        Message::Ping(_) => "Ping",
                                        Message::Pong(_) => "Pong",
                                        Message::Close(_) => "Close",
                                        Message::Frame(_) => "Frame",
                                    });
                            }

                            match &message {
                                Message::Ping(data) => {
                                    debug!("[SHARD-{}] Received ping, sending pong", self.shard_id);
                                    write.send(Message::Pong(data.clone())).await?;
                                }
                                Message::Pong(_) => {
                                    debug!("[SHARD-{}] Received pong", self.shard_id);
                                    health.record_pong_received();
                                    self.metrics.record_pong();
                                }
                                Message::Close(_) => {
                                    info!("[SHARD-{}] Received close frame", self.shard_id);
                                    break;
                                }
                                _ => {
                                    // Only count actual data messages for data timeout
                                    // (not ping/pong which keep connection alive but don't indicate data flow)
                                    health.record_data_received();

                                    // Check for application-level heartbeat
                                    if self.handler.is_heartbeat(&message) {
                                        debug!("[SHARD-{}] Received application heartbeat", self.shard_id);
                                    }

                                    // Validate sequence number before processing
                                    let validation = self.handler.validate_sequence(&message, state);
                                    if !validation.is_valid {
                                        match validation.recovery {
                                            SequenceRecovery::Continue => {
                                                // Log but continue processing
                                                debug!(
                                                    "[SHARD-{}] Sequence validation failed, continuing",
                                                    self.shard_id
                                                );
                                            }
                                            SequenceRecovery::DropMessage => {
                                                // Skip this message
                                                debug!(
                                                    "[SHARD-{}] Dropping message due to sequence gap",
                                                    self.shard_id
                                                );
                                                // Send resubscription if requested
                                                if !validation.resubscribe.is_empty() {
                                                    if let Some(resub_msg) = self.handler.resubscription_message(&validation.resubscribe) {
                                                        if let Err(e) = write.send(resub_msg).await {
                                                            warn!(
                                                                "[SHARD-{}] Failed to send resubscription: {}",
                                                                self.shard_id, e
                                                            );
                                                        } else {
                                                            info!(
                                                                "[SHARD-{}] Sent resubscription for {} items due to sequence gap",
                                                                self.shard_id, validation.resubscribe.len()
                                                            );
                                                        }
                                                    }
                                                }
                                                continue; // Skip on_message for this message
                                            }
                                            SequenceRecovery::Reconnect => {
                                                // Trigger reconnection
                                                warn!(
                                                    "[SHARD-{}] Sequence gap detected, reconnecting",
                                                    self.shard_id
                                                );
                                                break;
                                            }
                                        }
                                    }

                                    // Let handler process the message
                                    self.call_on_message(message, state).await;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            warn!("[SHARD-{}] WebSocket error: {}", self.shard_id, e);
                            return Err(Error::WebSocket(e));
                        }
                        None => {
                            info!("[SHARD-{}] WebSocket stream ended", self.shard_id);
                            break;
                        }
                    }
                }

                // Handle commands from manager
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(ConnectionCommand::Send(msg)) => {
                            if let Err(e) = write.send(msg).await {
                                warn!("[SHARD-{}] Failed to send message: {}", self.shard_id, e);
                                return Err(Error::WebSocket(e));
                            }
                            self.metrics.record_message_sent();
                        }
                        Some(ConnectionCommand::Close) => {
                            info!("[SHARD-{}] Received close command", self.shard_id);
                            let _ = write.send(Message::Close(None)).await;
                            should_stop = true;
                            break;
                        }
                        Some(ConnectionCommand::Reconnect) => {
                            info!("[SHARD-{}] Received reconnect command", self.shard_id);
                            let _ = write.send(Message::Close(None)).await;
                            break;
                        }
                        None => {
                            info!("[SHARD-{}] Command channel closed", self.shard_id);
                            should_stop = true;
                            break;
                        }
                    }
                }

                // Health monitoring with smart scheduling
                _ = tokio::time::sleep(next_health_check) => {
                    // Check if pong is overdue (this has side effects - only call once)
                    if health.check_and_record_pong_timeout() {
                        self.metrics.record_health_failure();
                        warn!(
                            "[SHARD-{}] Pong timeout (failures: {})",
                            self.shard_id,
                            health.consecutive_failures()
                        );

                        if health.is_unhealthy() {
                            warn!("[SHARD-{}] Connection unhealthy, reconnecting", self.shard_id);
                            break;
                        }
                    }

                    // Check data timeout
                    if health.is_data_timeout() {
                        self.metrics.record_health_failure();

                        // Request hot switchover if channel available, otherwise regular reconnect
                        if let Some(ref tx) = self.switchover_tx {
                            debug!("[SHARD-{}] Data timeout, requesting hot switchover", self.shard_id);
                            // Non-blocking send - if manager is busy, fall back to regular reconnect
                            if tx.try_send(self.shard_id).is_ok() {
                                // Continue running while manager performs switchover
                                // The manager will close this connection when new one is ready
                                health.reset_data_timeout();
                                continue;
                            }
                            debug!("[SHARD-{}] Hot switchover channel full, falling back to reconnect", self.shard_id);
                        } else {
                            warn!("[SHARD-{}] Data timeout, reconnecting", self.shard_id);
                        }
                        break;
                    }

                    // Send ping if needed
                    if health.should_send_ping() {
                        if let Err(e) = write.send(Message::Ping(ping_data.clone())).await {
                            warn!("[SHARD-{}] Failed to send ping: {}", self.shard_id, e);
                            break;
                        }
                        health.record_ping_sent();
                        self.metrics.record_ping();
                    }

                    // Check proactive reconnect
                    if let Some(interval) = proactive_reconnect_interval {
                        if connection_start.elapsed() >= interval {
                            info!(
                                "[SHARD-{}] Proactive reconnect after {:?}",
                                self.shard_id, interval
                            );
                            let _ = write.send(Message::Close(None)).await;
                            break;
                        }
                    }
                }
            }
        }

        // Update metrics on disconnect
        self.metrics.update_shard(self.shard_id, |s| {
            s.is_connected = false;
            if let Some(connected_at) = s.last_connected_at {
                s.total_uptime += connected_at.elapsed();
            }
        });

        // Notify handler
        self.handler.on_disconnect(state).await;

        Ok(should_stop)
    }
}

/// Type alias for WebSocket stream
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Connect to WebSocket with optional source IP binding, proxy support, and custom headers.
///
/// # Arguments
/// * `url` - WebSocket URL (wss:// or ws://)
/// * `source_ip` - Optional source IP to bind outgoing connection
/// * `proxy` - Optional proxy URL (socks5://host:port or http://host:port)
/// * `headers` - Additional headers to include in the connection request (e.g., auth)
async fn connect_with_options(
    url: &str,
    source_ip: Option<&str>,
    proxy: Option<&str>,
    headers: Vec<(HeaderName, HeaderValue)>,
) -> Result<WsStream, Error> {
    // Parse URL
    let parsed_url = Url::parse(url).map_err(|e| Error::ConnectionFailed {
        attempts: 0,
        last_error: format!("Invalid URL: {}", e),
    })?;

    let host = parsed_url
        .host_str()
        .ok_or_else(|| Error::ConnectionFailed {
            attempts: 0,
            last_error: "No host in URL".to_string(),
        })?;

    let is_tls = parsed_url.scheme() == "wss";
    let port = parsed_url.port().unwrap_or(if is_tls { 443 } else { 80 });

    // Build the WebSocket request
    let mut request = url
        .into_client_request()
        .map_err(|e| Error::ConnectionFailed {
            attempts: 0,
            last_error: format!("Invalid WebSocket request: {}", e),
        })?;

    // Add custom headers (e.g., authentication)
    for (name, value) in headers {
        request.headers_mut().insert(name, value);
    }

    // Connect based on proxy/source_ip configuration
    let tcp_stream = if let Some(proxy_url) = proxy {
        connect_via_proxy(proxy_url, host, port, source_ip).await?
    } else {
        connect_direct(host, port, source_ip).await?
    };

    // Set TCP options for low latency
    set_tcp_options(&tcp_stream);

    // TLS connector (if needed)
    let connector = if is_tls {
        let tls = native_tls::TlsConnector::new().map_err(|e| Error::ConnectionFailed {
            attempts: 0,
            last_error: format!("TLS error: {}", e),
        })?;
        Some(Connector::NativeTls(tls))
    } else {
        None
    };

    // WebSocket handshake
    let (ws_stream, _response) = client_async_tls_with_config(request, tcp_stream, None, connector)
        .await
        .map_err(Error::WebSocket)?;

    Ok(ws_stream)
}

/// Connect directly (optionally with source IP binding)
async fn connect_direct(
    host: &str,
    port: u16,
    source_ip: Option<&str>,
) -> Result<tokio::net::TcpStream, Error> {
    // DNS lookup
    let dest_str = format!("{}:{}", host, port);
    let dest_addr: SocketAddr = tokio::net::lookup_host(&dest_str)
        .await
        .map_err(|e| Error::ConnectionFailed {
            attempts: 0,
            last_error: format!("DNS lookup failed: {}", e),
        })?
        .next()
        .ok_or_else(|| Error::ConnectionFailed {
            attempts: 0,
            last_error: format!("No addresses found for {}", host),
        })?;

    // Create socket
    let socket = if dest_addr.is_ipv4() {
        TcpSocket::new_v4()
    } else {
        TcpSocket::new_v6()
    }
    .map_err(|e| Error::ConnectionFailed {
        attempts: 0,
        last_error: format!("Failed to create socket: {}", e),
    })?;

    // Bind to source IP if specified
    if let Some(ip) = source_ip {
        let source_addr: SocketAddr =
            format!("{}:0", ip)
                .parse()
                .map_err(|e| Error::ConnectionFailed {
                    attempts: 0,
                    last_error: format!("Invalid source IP '{}': {}", ip, e),
                })?;
        socket
            .bind(source_addr)
            .map_err(|e| Error::ConnectionFailed {
                attempts: 0,
                last_error: format!("Failed to bind to {}: {}", ip, e),
            })?;
    }

    // Connect
    socket
        .connect(dest_addr)
        .await
        .map_err(|e| Error::ConnectionFailed {
            attempts: 0,
            last_error: format!("TCP connect to {} failed: {}", dest_addr, e),
        })
}

/// Connect via SOCKS5 or HTTP CONNECT proxy
async fn connect_via_proxy(
    proxy_url: &str,
    target_host: &str,
    target_port: u16,
    source_ip: Option<&str>,
) -> Result<tokio::net::TcpStream, Error> {
    let parsed = Url::parse(proxy_url).map_err(|e| Error::ConnectionFailed {
        attempts: 0,
        last_error: format!("Invalid proxy URL: {}", e),
    })?;

    let proxy_host = parsed.host_str().ok_or_else(|| Error::ConnectionFailed {
        attempts: 0,
        last_error: "No host in proxy URL".to_string(),
    })?;
    let proxy_port = parsed.port().unwrap_or(1080);

    match parsed.scheme() {
        "socks5" | "socks5h" => {
            // SOCKS5 proxy - tokio-socks handles the connection internally
            // Note: source_ip is not supported with SOCKS5 (tokio-socks doesn't support it)
            if source_ip.is_some() {
                warn!("source_ip is not supported with SOCKS5 proxy, ignoring");
            }

            let proxy_addr = (proxy_host, proxy_port);
            let target = (target_host, target_port);

            let socks_stream = if !parsed.username().is_empty() {
                // With authentication
                tokio_socks::tcp::Socks5Stream::connect_with_password(
                    proxy_addr,
                    target,
                    parsed.username(),
                    parsed.password().unwrap_or(""),
                )
                .await
            } else {
                // Without authentication
                tokio_socks::tcp::Socks5Stream::connect(proxy_addr, target).await
            }
            .map_err(|e| Error::ConnectionFailed {
                attempts: 0,
                last_error: format!("SOCKS5 connect failed: {}", e),
            })?;

            Ok(socks_stream.into_inner())
        }
        "http" | "https" => {
            // HTTP CONNECT tunnel - we can use source IP binding here
            use tokio::io::{AsyncWriteExt, BufReader};

            // Security limits for proxy response parsing
            const MAX_HEADERS: usize = 100;
            const MAX_TOTAL_HEADER_SIZE: usize = 16 * 1024; // 16KB
            const MAX_LINE_LENGTH: usize = 8 * 1024; // 8KB per line
            const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

            // First connect to proxy (with optional source IP)
            let proxy_stream = connect_direct(proxy_host, proxy_port, source_ip).await?;
            let mut stream = proxy_stream;

            // Send CONNECT request
            let connect_req = if !parsed.username().is_empty() {
                let auth = format!("{}:{}", parsed.username(), parsed.password().unwrap_or(""));
                let encoded = base64_encode(&auth);
                format!(
                    "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\nProxy-Authorization: Basic {}\r\n\r\n",
                    target_host, target_port, target_host, target_port, encoded
                )
            } else {
                format!(
                    "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
                    target_host, target_port, target_host, target_port
                )
            };

            stream
                .write_all(connect_req.as_bytes())
                .await
                .map_err(|e| Error::ConnectionFailed {
                    attempts: 0,
                    last_error: format!("Failed to send CONNECT: {}", e),
                })?;

            // Read and parse response with timeout and limits
            let read_response = async {
                let mut reader = BufReader::new(&mut stream);
                let mut response_line = String::new();
                let mut total_bytes_read: usize = 0;

                // Read status line with length limit
                let bytes_read =
                    read_line_limited(&mut reader, &mut response_line, MAX_LINE_LENGTH)
                        .await
                        .map_err(|e| Error::ConnectionFailed {
                            attempts: 0,
                            last_error: format!("Failed to read CONNECT response: {}", e),
                        })?;

                if bytes_read == 0 {
                    return Err(Error::ConnectionFailed {
                        attempts: 0,
                        last_error: "Proxy closed connection before sending response".to_string(),
                    });
                }
                total_bytes_read += bytes_read;

                // Parse HTTP status line properly: "HTTP/1.x STATUS_CODE REASON"
                let status_code = parse_http_status_line(&response_line).ok_or_else(|| {
                    Error::ConnectionFailed {
                        attempts: 0,
                        last_error: format!(
                            "Invalid HTTP response from proxy: {}",
                            response_line.trim()
                        ),
                    }
                })?;

                if status_code != 200 {
                    return Err(Error::ConnectionFailed {
                        attempts: 0,
                        last_error: format!(
                            "Proxy CONNECT failed with status {}: {}",
                            status_code,
                            response_line.trim()
                        ),
                    });
                }

                // Read headers until empty line (end of headers)
                // Apply limits: max headers, max total size
                let mut header_count = 0;
                loop {
                    if header_count >= MAX_HEADERS {
                        return Err(Error::ConnectionFailed {
                            attempts: 0,
                            last_error: format!(
                                "Proxy sent too many headers (>{} headers)",
                                MAX_HEADERS
                            ),
                        });
                    }

                    if total_bytes_read >= MAX_TOTAL_HEADER_SIZE {
                        return Err(Error::ConnectionFailed {
                            attempts: 0,
                            last_error: format!(
                                "Proxy headers too large (>{} bytes)",
                                MAX_TOTAL_HEADER_SIZE
                            ),
                        });
                    }

                    let mut line = String::new();
                    let bytes_read = read_line_limited(&mut reader, &mut line, MAX_LINE_LENGTH)
                        .await
                        .map_err(|e| Error::ConnectionFailed {
                            attempts: 0,
                            last_error: format!("Failed to read proxy headers: {}", e),
                        })?;

                    if bytes_read == 0 {
                        return Err(Error::ConnectionFailed {
                            attempts: 0,
                            last_error: "Proxy closed connection during header reading".to_string(),
                        });
                    }

                    total_bytes_read += bytes_read;
                    header_count += 1;

                    if line.trim().is_empty() {
                        break;
                    }
                }

                Ok(())
            };

            // Apply timeout to entire header reading operation
            timeout(HEADER_READ_TIMEOUT, read_response)
                .await
                .map_err(|_| Error::ConnectionFailed {
                    attempts: 0,
                    last_error: format!(
                        "Timeout reading proxy response (>{:?})",
                        HEADER_READ_TIMEOUT
                    ),
                })??;

            // Return the underlying stream (tunnel established)
            Ok(stream)
        }
        scheme => Err(Error::ConnectionFailed {
            attempts: 0,
            last_error: format!("Unsupported proxy scheme: {}", scheme),
        }),
    }
}

/// Set TCP options for low latency
fn set_tcp_options(stream: &tokio::net::TcpStream) {
    // Get the underlying socket
    let sock2 = socket2::SockRef::from(stream);

    // Enable TCP_NODELAY (disable Nagle's algorithm)
    let _ = sock2.set_nodelay(true);

    // Set keepalive to detect dead connections
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(Duration::from_secs(30))
        .with_interval(Duration::from_secs(10));
    let _ = sock2.set_tcp_keepalive(&keepalive);
}

/// Simple base64 encoding for proxy auth
fn base64_encode(input: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut result = String::new();

    for chunk in bytes.chunks(3) {
        let n = match chunk.len() {
            3 => ((chunk[0] as u32) << 16) | ((chunk[1] as u32) << 8) | (chunk[2] as u32),
            2 => ((chunk[0] as u32) << 16) | ((chunk[1] as u32) << 8),
            1 => (chunk[0] as u32) << 16,
            _ => continue,
        };

        result.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        result.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((n >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[(n & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }

    result
}

/// Parse HTTP status line and extract status code.
///
/// Expects format: "HTTP/1.x STATUS_CODE [REASON_PHRASE]\r\n"
/// Returns None if the format is invalid.
fn parse_http_status_line(line: &str) -> Option<u16> {
    let line = line.trim();

    // Split on whitespace
    let mut parts = line.split_whitespace();

    // First part must be HTTP version (HTTP/1.0 or HTTP/1.1)
    let version = parts.next()?;
    if !version.starts_with("HTTP/1.") {
        return None;
    }

    // Validate version suffix is 0 or 1
    let version_suffix = version.strip_prefix("HTTP/1.")?;
    if version_suffix != "0" && version_suffix != "1" {
        return None;
    }

    // Second part is the status code
    let status_str = parts.next()?;
    let status_code: u16 = status_str.parse().ok()?;

    // Validate status code is in valid HTTP range (100-599)
    if !(100..=599).contains(&status_code) {
        return None;
    }

    Some(status_code)
}

/// Read a line with a maximum length limit.
///
/// Returns the number of bytes read, or an error if the line exceeds the limit.
/// This prevents memory exhaustion from malicious/malformed responses.
async fn read_line_limited<R: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut R,
    buf: &mut String,
    max_length: usize,
) -> std::io::Result<usize> {
    use tokio::io::AsyncBufReadExt;

    let mut total_read = 0;

    loop {
        let available = reader.fill_buf().await?;

        if available.is_empty() {
            // EOF reached
            return Ok(total_read);
        }

        // Find newline in available buffer
        let newline_pos = available.iter().position(|&b| b == b'\n');

        let (to_consume, done) = match newline_pos {
            Some(pos) => (pos + 1, true), // Include the newline
            None => (available.len(), false),
        };

        // Check if this would exceed our limit
        if total_read + to_consume > max_length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Line exceeds maximum length of {} bytes", max_length),
            ));
        }

        // Convert to string and append
        let chunk = &available[..to_consume];
        let chunk_str = std::str::from_utf8(chunk).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid UTF-8: {}", e),
            )
        })?;
        buf.push_str(chunk_str);
        total_read += to_consume;

        reader.consume(to_consume);

        if done {
            return Ok(total_read);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64_encode() {
        assert_eq!(base64_encode("user:pass"), "dXNlcjpwYXNz");
        assert_eq!(base64_encode("hello"), "aGVsbG8=");
        assert_eq!(base64_encode("a"), "YQ==");
    }

    #[test]
    fn test_parse_http_status_line_valid() {
        // Standard responses
        assert_eq!(parse_http_status_line("HTTP/1.1 200 OK\r\n"), Some(200));
        assert_eq!(parse_http_status_line("HTTP/1.0 200 OK\r\n"), Some(200));
        assert_eq!(
            parse_http_status_line("HTTP/1.1 200 Connection established\r\n"),
            Some(200)
        );

        // Without reason phrase
        assert_eq!(parse_http_status_line("HTTP/1.1 200\r\n"), Some(200));
        assert_eq!(parse_http_status_line("HTTP/1.1 200"), Some(200));

        // Various status codes
        assert_eq!(
            parse_http_status_line("HTTP/1.1 407 Proxy Authentication Required\r\n"),
            Some(407)
        );
        assert_eq!(
            parse_http_status_line("HTTP/1.1 502 Bad Gateway\r\n"),
            Some(502)
        );
        assert_eq!(
            parse_http_status_line("HTTP/1.1 100 Continue\r\n"),
            Some(100)
        );
        assert_eq!(parse_http_status_line("HTTP/1.1 599 Custom\r\n"), Some(599));
    }

    #[test]
    fn test_parse_http_status_line_invalid() {
        // Not HTTP/1.x
        assert_eq!(parse_http_status_line("HTTP/2.0 200 OK\r\n"), None);
        assert_eq!(parse_http_status_line("HTTP/0.9 200 OK\r\n"), None);
        assert_eq!(parse_http_status_line("HTTP/1.2 200 OK\r\n"), None);

        // Missing version
        assert_eq!(parse_http_status_line("200 OK\r\n"), None);

        // Missing status code
        assert_eq!(parse_http_status_line("HTTP/1.1\r\n"), None);
        assert_eq!(parse_http_status_line("HTTP/1.1 \r\n"), None);

        // Invalid status code (not a number)
        assert_eq!(parse_http_status_line("HTTP/1.1 OK\r\n"), None);
        assert_eq!(parse_http_status_line("HTTP/1.1 2xx OK\r\n"), None);

        // Status code out of range
        assert_eq!(parse_http_status_line("HTTP/1.1 99 Too Low\r\n"), None);
        assert_eq!(parse_http_status_line("HTTP/1.1 600 Too High\r\n"), None);

        // Empty/garbage
        assert_eq!(parse_http_status_line(""), None);
        assert_eq!(parse_http_status_line("garbage"), None);

        // Tricky strings that contain "200" but aren't valid
        assert_eq!(parse_http_status_line("Error 200 not found"), None);
        assert_eq!(parse_http_status_line("Status: 200"), None);
    }

    #[tokio::test]
    async fn test_read_line_limited_normal() {
        use tokio::io::BufReader;

        let data = b"HTTP/1.1 200 OK\r\nHeader: value\r\n";
        let mut reader = BufReader::new(&data[..]);
        let mut buf = String::new();

        let bytes = read_line_limited(&mut reader, &mut buf, 1024)
            .await
            .unwrap();
        assert_eq!(bytes, 17);
        assert_eq!(buf, "HTTP/1.1 200 OK\r\n");

        buf.clear();
        let bytes = read_line_limited(&mut reader, &mut buf, 1024)
            .await
            .unwrap();
        assert_eq!(bytes, 15);
        assert_eq!(buf, "Header: value\r\n");
    }

    #[tokio::test]
    async fn test_read_line_limited_exceeds_limit() {
        use tokio::io::BufReader;

        let data = b"This is a very long line that exceeds our tiny limit\r\n";
        let mut reader = BufReader::new(&data[..]);
        let mut buf = String::new();

        let result = read_line_limited(&mut reader, &mut buf, 10).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn test_read_line_limited_eof() {
        use tokio::io::BufReader;

        let data = b"no newline";
        let mut reader = BufReader::new(&data[..]);
        let mut buf = String::new();

        let bytes = read_line_limited(&mut reader, &mut buf, 1024)
            .await
            .unwrap();
        assert_eq!(bytes, 10);
        assert_eq!(buf, "no newline");
    }

    #[tokio::test]
    async fn test_read_line_limited_empty() {
        use tokio::io::BufReader;

        let data = b"";
        let mut reader = BufReader::new(&data[..]);
        let mut buf = String::new();

        let bytes = read_line_limited(&mut reader, &mut buf, 1024)
            .await
            .unwrap();
        assert_eq!(bytes, 0);
        assert_eq!(buf, "");
    }
}
