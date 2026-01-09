use crate::config::{BackoffConfig, ConnectionConfig, HealthConfig};
use crate::error::Error;
use crate::handler::{ConnectionState, WebSocketHandler};
use crate::health::HealthMonitor;
use crate::metrics::Metrics;
use futures_util::{SinkExt, StreamExt};
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Duration};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

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
}

impl<H: WebSocketHandler> Connection<H> {
    /// Create a new connection manager
    pub fn new(
        shard_id: usize,
        handler: Arc<H>,
        config: ConnectionConfig,
        backoff: BackoffConfig,
        health_config: HealthConfig,
        metrics: Arc<Metrics>,
        command_rx: mpsc::Receiver<ConnectionCommand>,
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
        }
    }

    /// Create a new connection with a ready signal for hot switchover
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
                    let remaining = self.config.circuit_breaker_reset_timeout - tripped_at.elapsed();
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
                    self.shard_id, delay, reconnect_attempt + 1
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
                    warn!(
                        "[SHARD-{}] Connection error: {} (attempt {}, consecutive failures: {})",
                        self.shard_id, e, reconnect_attempt + 1, consecutive_failures
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
                    warn!(
                        "[SHARD-{}] Handler.on_error task failed: {:?}",
                        shard_id, e
                    );
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
        if self.config.low_latency_mode {
            // Direct call - no spawn overhead (~1-5µs savings per message)
            self.handler.on_message(message, state).await;
        } else {
            // Spawn for panic protection
            let handler = self.handler.clone();
            let state_clone = state.clone();
            let shard_id = self.shard_id;

            let result = tokio::task::spawn(async move {
                let fut = AssertUnwindSafe(handler.on_message(message, &state_clone));
                fut.await
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
        debug!("[SHARD-{}] Connecting to {}", self.shard_id, url);

        // Connect with timeout
        let ws_stream = match timeout(
            self.config.connect_timeout,
            connect_async(&url),
        )
        .await
        {
            Ok(Ok((stream, _response))) => stream,
            Ok(Err(e)) => return Err(Error::WebSocket(e)),
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

        info!("[SHARD-{}] Connected to {}", self.shard_id, url);

        let (mut write, mut read) = ws_stream.split();

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
                            health.record_data_received();
                            self.metrics.record_message_received();

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
                                    // Check for application-level heartbeat
                                    if self.handler.is_heartbeat(&message) {
                                        debug!("[SHARD-{}] Received application heartbeat", self.shard_id);
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
                        warn!("[SHARD-{}] Data timeout, reconnecting", self.shard_id);
                        self.metrics.record_health_failure();
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

#[cfg(test)]
mod tests {
    // Integration tests would go here with ws_mock
}
