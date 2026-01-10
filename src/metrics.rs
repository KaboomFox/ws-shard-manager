use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Atomic storage for per-shard last message timestamp.
/// Uses milliseconds since a baseline instant for efficient atomic updates.
#[derive(Debug)]
struct AtomicLastMessage {
    /// Milliseconds since baseline, or 0 if never received
    millis_since_baseline: AtomicU64,
    /// Baseline instant (set once at creation)
    baseline: Instant,
}

impl AtomicLastMessage {
    fn new() -> Self {
        Self {
            millis_since_baseline: AtomicU64::new(0),
            baseline: Instant::now(),
        }
    }

    /// Record that a message was received now
    #[inline]
    fn record_now(&self) {
        let millis = self.baseline.elapsed().as_millis() as u64;
        // Use saturating_add to prevent overflow (would take ~584 million years)
        self.millis_since_baseline.store(millis.saturating_add(1), Ordering::Release);
    }

    /// Get elapsed time since last message, or None if no message received
    #[inline]
    fn elapsed(&self) -> Option<Duration> {
        let stored = self.millis_since_baseline.load(Ordering::Acquire);
        if stored == 0 {
            return None;
        }
        // stored is millis + 1, so subtract 1
        let last_message_millis = stored - 1;
        let now_millis = self.baseline.elapsed().as_millis() as u64;
        let elapsed_millis = now_millis.saturating_sub(last_message_millis);
        Some(Duration::from_millis(elapsed_millis))
    }
}

/// Metrics for observability.
///
/// This struct provides counters and gauges for monitoring WebSocket health.
/// Use `snapshot()` to get a point-in-time view of all metrics, or use
/// individual getter methods for specific values.
///
/// # Thread Safety
///
/// `Metrics` is `Send + Sync` and all methods are safe to call from multiple
/// tasks concurrently. Counters use atomic operations, and per-shard metrics
/// are protected by `parking_lot::RwLock`.
///
/// # Example
/// ```ignore
/// let metrics = manager.metrics();
///
/// // Get individual values
/// println!("Connections: {}", metrics.connections());
/// println!("Messages received: {}", metrics.messages_received());
///
/// // Get full snapshot for export
/// let snapshot = metrics.snapshot();
/// ```
#[derive(Debug, Default)]
pub struct Metrics {
    // Counter fields - private, exposed via getters
    connections_total: AtomicU64,
    reconnections_total: AtomicU64,
    messages_received_total: AtomicU64,
    messages_sent_total: AtomicU64,
    errors_total: AtomicU64,
    pings_sent_total: AtomicU64,
    pongs_received_total: AtomicU64,
    health_failures_total: AtomicU64,
    rebalances_total: AtomicU64,
    hot_switchovers_total: AtomicU64,
    hot_switchover_failures_total: AtomicU64,
    subscription_send_failures_total: AtomicU64,
    circuit_breaker_trips_total: AtomicU64,

    /// Per-shard metrics
    shard_metrics: RwLock<Vec<ShardMetrics>>,

    /// Per-shard atomic last message timestamps (lock-free updates)
    /// Indexed by shard_id. Grows as needed via RwLock.
    shard_last_message: RwLock<Vec<AtomicLastMessage>>,
}

/// Metrics for a single shard
#[derive(Debug, Clone)]
pub struct ShardMetrics {
    /// Shard identifier
    pub shard_id: usize,
    /// Current number of subscriptions
    pub subscription_count: usize,
    /// Whether the shard is currently connected
    pub is_connected: bool,
    /// Duration since last successful connection (None if never connected)
    pub time_since_connected: Option<Duration>,
    /// Duration since last message received (None if no messages)
    pub time_since_last_message: Option<Duration>,
    /// Current reconnection attempt (0 if connected)
    pub reconnect_attempt: u32,
    /// Total uptime for this shard
    pub total_uptime: Duration,
    // Internal fields for tracking (not exposed in snapshot)
    #[doc(hidden)]
    pub(crate) last_connected_at: Option<Instant>,
    #[doc(hidden)]
    pub(crate) last_message_at: Option<Instant>,
}

impl Default for ShardMetrics {
    fn default() -> Self {
        Self {
            shard_id: 0,
            subscription_count: 0,
            is_connected: false,
            time_since_connected: None,
            time_since_last_message: None,
            reconnect_attempt: 0,
            total_uptime: Duration::ZERO,
            last_connected_at: None,
            last_message_at: None,
        }
    }
}

impl ShardMetrics {
    /// Create a snapshot with computed durations
    fn snapshot(&self) -> ShardMetrics {
        ShardMetrics {
            shard_id: self.shard_id,
            subscription_count: self.subscription_count,
            is_connected: self.is_connected,
            time_since_connected: self.last_connected_at.map(|t| t.elapsed()),
            time_since_last_message: self.last_message_at.map(|t| t.elapsed()),
            reconnect_attempt: self.reconnect_attempt,
            total_uptime: self.total_uptime,
            last_connected_at: self.last_connected_at,
            last_message_at: self.last_message_at,
        }
    }
}

impl Metrics {
    /// Create a new Metrics instance
    pub fn new() -> Self {
        Self::default()
    }

    // ========== Getters ==========

    /// Get total connections established
    pub fn connections(&self) -> u64 {
        self.connections_total.load(Ordering::Relaxed)
    }

    /// Get total reconnections
    pub fn reconnections(&self) -> u64 {
        self.reconnections_total.load(Ordering::Relaxed)
    }

    /// Get total messages received
    pub fn messages_received(&self) -> u64 {
        self.messages_received_total.load(Ordering::Relaxed)
    }

    /// Get total messages sent
    pub fn messages_sent(&self) -> u64 {
        self.messages_sent_total.load(Ordering::Relaxed)
    }

    /// Get total errors
    pub fn errors(&self) -> u64 {
        self.errors_total.load(Ordering::Relaxed)
    }

    /// Get total pings sent
    pub fn pings_sent(&self) -> u64 {
        self.pings_sent_total.load(Ordering::Relaxed)
    }

    /// Get total pongs received
    pub fn pongs_received(&self) -> u64 {
        self.pongs_received_total.load(Ordering::Relaxed)
    }

    /// Get total health check failures
    pub fn health_failures(&self) -> u64 {
        self.health_failures_total.load(Ordering::Relaxed)
    }

    /// Get total rebalance operations
    pub fn rebalances(&self) -> u64 {
        self.rebalances_total.load(Ordering::Relaxed)
    }

    /// Get total hot switchovers
    pub fn hot_switchovers(&self) -> u64 {
        self.hot_switchovers_total.load(Ordering::Relaxed)
    }

    /// Get total hot switchover failures
    pub fn hot_switchover_failures(&self) -> u64 {
        self.hot_switchover_failures_total.load(Ordering::Relaxed)
    }

    /// Get total subscription send failures
    pub fn subscription_send_failures(&self) -> u64 {
        self.subscription_send_failures_total.load(Ordering::Relaxed)
    }

    /// Get total circuit breaker trips
    pub fn circuit_breaker_trips(&self) -> u64 {
        self.circuit_breaker_trips_total.load(Ordering::Relaxed)
    }

    // ========== Recording methods (called internally) ==========

    /// Increment connection counter
    pub(crate) fn record_connection(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment reconnection counter
    pub(crate) fn record_reconnection(&self) {
        self.reconnections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment message received counter
    pub(crate) fn record_message_received(&self) {
        self.messages_received_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record message received for a specific shard (lock-free fast path).
    ///
    /// Updates the shard's `last_message_at` timestamp atomically without acquiring
    /// a write lock. This is critical for performance as it's called on every message.
    #[inline]
    pub(crate) fn record_shard_message_received(&self, shard_id: usize) {
        // Fast path: try to update existing atomic
        {
            let atomics = self.shard_last_message.read();
            if let Some(atomic) = atomics.get(shard_id) {
                atomic.record_now();
                return;
            }
        }

        // Slow path: need to grow the vector
        let mut atomics = self.shard_last_message.write();
        while atomics.len() <= shard_id {
            atomics.push(AtomicLastMessage::new());
        }
        atomics[shard_id].record_now();
    }

    /// Get time since last message for a shard (lock-free read).
    ///
    /// Returns `None` if the shard doesn't exist or has never received a message.
    #[inline]
    pub fn shard_time_since_last_message(&self, shard_id: usize) -> Option<Duration> {
        let atomics = self.shard_last_message.read();
        atomics.get(shard_id).and_then(|a| a.elapsed())
    }

    /// Increment message sent counter
    pub(crate) fn record_message_sent(&self) {
        self.messages_sent_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment error counter
    pub(crate) fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment ping counter
    pub(crate) fn record_ping(&self) {
        self.pings_sent_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment pong counter
    pub(crate) fn record_pong(&self) {
        self.pongs_received_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment health failure counter
    pub(crate) fn record_health_failure(&self) {
        self.health_failures_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment rebalance counter
    pub(crate) fn record_rebalance(&self) {
        self.rebalances_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment hot switchover counter
    pub(crate) fn record_hot_switchover(&self) {
        self.hot_switchovers_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment hot switchover failure counter
    pub(crate) fn record_hot_switchover_failed(&self) {
        self.hot_switchover_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment subscription send failure counter
    pub(crate) fn record_subscription_send_failed(&self) {
        self.subscription_send_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Increment circuit breaker trip counter
    pub(crate) fn record_circuit_breaker_trip(&self) {
        self.circuit_breaker_trips_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Update metrics for a specific shard
    pub(crate) fn update_shard(&self, shard_id: usize, update_fn: impl FnOnce(&mut ShardMetrics)) {
        let mut shards = self.shard_metrics.write();

        // Ensure we have enough entries
        while shards.len() <= shard_id {
            let id = shards.len();
            shards.push(ShardMetrics {
                shard_id: id,
                ..Default::default()
            });
        }

        update_fn(&mut shards[shard_id]);
    }

    /// Get a snapshot of all shard metrics with computed durations
    pub fn shard_metrics(&self) -> Vec<ShardMetrics> {
        let shards = self.shard_metrics.read();
        let atomics = self.shard_last_message.read();

        shards
            .iter()
            .enumerate()
            .map(|(idx, s)| {
                let mut snapshot = s.snapshot();
                // Override time_since_last_message with atomic value
                snapshot.time_since_last_message = atomics.get(idx).and_then(|a| a.elapsed());
                snapshot
            })
            .collect()
    }

    /// Get a single shard's subscription count without cloning all metrics.
    /// Returns 0 if the shard doesn't exist.
    #[inline]
    pub fn shard_subscription_count(&self, shard_id: usize) -> usize {
        self.shard_metrics
            .read()
            .get(shard_id)
            .map(|s| s.subscription_count)
            .unwrap_or(0)
    }

    /// Get current active connection count
    pub fn active_connections(&self) -> usize {
        self.shard_metrics
            .read()
            .iter()
            .filter(|s| s.is_connected)
            .count()
    }

    /// Get total subscription count across all shards
    pub fn total_subscriptions(&self) -> usize {
        self.shard_metrics
            .read()
            .iter()
            .map(|s| s.subscription_count)
            .sum()
    }

    /// Get a point-in-time snapshot of all metrics for export
    ///
    /// This is the recommended way to get metrics for monitoring systems.
    pub fn snapshot(&self) -> MetricsSnapshot {
        // Take the shard lock once to ensure consistency
        let shards = self.shard_metrics.read();
        let atomics = self.shard_last_message.read();
        let shard_snapshots: Vec<ShardMetrics> = shards
            .iter()
            .enumerate()
            .map(|(idx, s)| {
                let mut snapshot = s.snapshot();
                // Override time_since_last_message with atomic value
                snapshot.time_since_last_message = atomics.get(idx).and_then(|a| a.elapsed());
                snapshot
            })
            .collect();

        MetricsSnapshot {
            connections_total: self.connections_total.load(Ordering::Acquire),
            reconnections_total: self.reconnections_total.load(Ordering::Acquire),
            messages_received_total: self.messages_received_total.load(Ordering::Acquire),
            messages_sent_total: self.messages_sent_total.load(Ordering::Acquire),
            errors_total: self.errors_total.load(Ordering::Acquire),
            pings_sent_total: self.pings_sent_total.load(Ordering::Acquire),
            pongs_received_total: self.pongs_received_total.load(Ordering::Acquire),
            health_failures_total: self.health_failures_total.load(Ordering::Acquire),
            rebalances_total: self.rebalances_total.load(Ordering::Acquire),
            hot_switchovers_total: self.hot_switchovers_total.load(Ordering::Acquire),
            hot_switchover_failures_total: self.hot_switchover_failures_total.load(Ordering::Acquire),
            subscription_send_failures_total: self.subscription_send_failures_total.load(Ordering::Acquire),
            circuit_breaker_trips_total: self.circuit_breaker_trips_total.load(Ordering::Acquire),
            active_connections: shard_snapshots.iter().filter(|s| s.is_connected).count(),
            total_subscriptions: shard_snapshots.iter().map(|s| s.subscription_count).sum(),
            shards: shard_snapshots,
        }
    }
}

/// A point-in-time snapshot of all metrics.
///
/// Use [`Metrics::snapshot()`] to get a consistent view of all metrics at once.
/// This is the recommended way to export metrics to monitoring systems.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Total number of WebSocket connections established (including reconnections)
    pub connections_total: u64,
    /// Total number of reconnection attempts after disconnection
    pub reconnections_total: u64,
    /// Total number of WebSocket messages received across all shards
    pub messages_received_total: u64,
    /// Total number of WebSocket messages sent across all shards
    pub messages_sent_total: u64,
    /// Total number of errors encountered (connection failures, panics, etc.)
    pub errors_total: u64,
    /// Total number of WebSocket ping frames sent for health monitoring
    pub pings_sent_total: u64,
    /// Total number of WebSocket pong frames received
    pub pongs_received_total: u64,
    /// Total number of health check failures (pong timeouts, data timeouts)
    pub health_failures_total: u64,
    /// Total number of shard rebalancing operations (new shard creation)
    pub rebalances_total: u64,
    /// Total number of hot switchover operations initiated
    pub hot_switchovers_total: u64,
    /// Total number of hot switchover operations that failed
    pub hot_switchover_failures_total: u64,
    /// Total number of subscription message send failures
    pub subscription_send_failures_total: u64,
    /// Total number of times the circuit breaker tripped due to consecutive failures
    pub circuit_breaker_trips_total: u64,
    /// Current number of active (connected) shards
    pub active_connections: usize,
    /// Current total number of subscriptions across all shards
    pub total_subscriptions: usize,
    /// Per-shard metrics with detailed connection state
    pub shards: Vec<ShardMetrics>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_counters() {
        let metrics = Metrics::new();

        metrics.record_connection();
        metrics.record_connection();
        metrics.record_reconnection();

        assert_eq!(metrics.connections(), 2);
        assert_eq!(metrics.reconnections(), 1);
    }

    #[test]
    fn test_shard_metrics() {
        let metrics = Metrics::new();

        metrics.update_shard(0, |s| {
            s.is_connected = true;
            s.subscription_count = 100;
        });

        metrics.update_shard(1, |s| {
            s.is_connected = true;
            s.subscription_count = 200;
        });

        assert_eq!(metrics.active_connections(), 2);
        assert_eq!(metrics.total_subscriptions(), 300);
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = Metrics::new();

        metrics.record_connection();
        metrics.update_shard(0, |s| {
            s.is_connected = true;
            s.subscription_count = 50;
        });

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.connections_total, 1);
        assert_eq!(snapshot.active_connections, 1);
        assert_eq!(snapshot.total_subscriptions, 50);
    }

    #[test]
    fn test_individual_getters() {
        let metrics = Metrics::new();

        metrics.record_connection();
        metrics.record_message_received();
        metrics.record_message_received();
        metrics.record_error();

        assert_eq!(metrics.connections(), 1);
        assert_eq!(metrics.messages_received(), 2);
        assert_eq!(metrics.errors(), 1);
        assert_eq!(metrics.messages_sent(), 0);
    }

    #[test]
    fn test_shard_message_received_updates_last_message_time() {
        let metrics = Metrics::new();

        // Initially no message time
        assert!(metrics.shard_time_since_last_message(0).is_none());

        // Record a message
        metrics.record_shard_message_received(0);

        // Now we should have a time
        let elapsed = metrics.shard_time_since_last_message(0);
        assert!(elapsed.is_some());
        assert!(elapsed.unwrap() < Duration::from_secs(1));

        // Verify it shows up in shard_metrics snapshot
        metrics.update_shard(0, |s| s.is_connected = true);
        let shard_snapshots = metrics.shard_metrics();
        assert!(!shard_snapshots.is_empty());
        assert!(shard_snapshots[0].time_since_last_message.is_some());
        assert!(shard_snapshots[0].time_since_last_message.unwrap() < Duration::from_secs(1));
    }

    #[test]
    fn test_shard_message_received_grows_vector() {
        let metrics = Metrics::new();

        // Record for shard 5 (should grow the vector)
        metrics.record_shard_message_received(5);

        assert!(metrics.shard_time_since_last_message(5).is_some());
        // Previous shards should still be None
        assert!(metrics.shard_time_since_last_message(0).is_none());
        assert!(metrics.shard_time_since_last_message(4).is_none());
    }
}
