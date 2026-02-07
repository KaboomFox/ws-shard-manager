use crate::config::ShardManagerConfig;
use crate::connection::{Connection, ConnectionCommand};
use crate::error::{Error, SubscribeResult};
use crate::handler::WebSocketHandler;
use crate::metrics::Metrics;
use crate::shard::{select_shard_excluding, Shard};
use futures_util::FutureExt;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::Duration;
use tracing::{debug, error, info, trace, warn};

/// Default channel buffer size
const DEFAULT_CHANNEL_SIZE: usize = 100;

/// Type alias for shard unsubscription data: (shard_id, items, sender, new_count)
type ShardUnsubData<S> = (usize, Vec<S>, mpsc::Sender<ConnectionCommand>, usize);

/// Timeout for waiting for new connection during hot switchover
const HOT_SWITCHOVER_TIMEOUT: Duration = Duration::from_secs(10);

/// Manages multiple WebSocket shards with auto-rebalancing and hot switchover.
///
/// # Thread Safety
///
/// `ShardManager` is `Send + Sync` and all methods can be safely called from
/// multiple tasks concurrently. Internal state is protected by `parking_lot::RwLock`
/// which does not poison on panic.
pub struct ShardManager<H: WebSocketHandler> {
    handler: Arc<H>,
    config: ShardManagerConfig,
    metrics: Arc<Metrics>,
    state: Arc<RwLock<ManagerState<H::Subscription>>>,
    /// Shard task handles indexed by shard ID (not position) to handle ID/index mismatch
    shard_handles: RwLock<HashMap<usize, JoinHandle<()>>>,
    /// Monotonically increasing counter for shard IDs to prevent race conditions
    next_shard_id: AtomicUsize,
    /// Mutex to serialize start/stop operations and prevent race conditions.
    /// This is a tokio::Mutex so it can be held across await points.
    lifecycle_lock: Mutex<()>,
    /// Channel for connections to request hot switchover
    switchover_tx: mpsc::Sender<usize>,
    /// Receiver for switchover requests (wrapped in Mutex for interior mutability)
    switchover_rx: Mutex<mpsc::Receiver<usize>>,
}

struct ManagerState<S: Clone + Eq + std::hash::Hash> {
    /// Shards indexed by their ID (not by position) to handle ID/index mismatch after stop/restart
    shards: HashMap<usize, Shard<S>>,
    subscription_to_shard: HashMap<S, usize>,
    last_shard_used: usize,
    is_running: bool,
    /// Shards currently undergoing hot switchover (blocked for new subscriptions)
    shards_in_switchover: HashSet<usize>,
    /// Shard IDs currently being created (to prevent race conditions)
    shards_being_created: HashSet<usize>,
}

impl<S: Clone + Eq + std::hash::Hash> Default for ManagerState<S> {
    fn default() -> Self {
        Self {
            shards: HashMap::new(),
            subscription_to_shard: HashMap::new(),
            last_shard_used: 0,
            is_running: false,
            shards_in_switchover: HashSet::new(),
            shards_being_created: HashSet::new(),
        }
    }
}

/// Guard that removes a shard from the switchover set on drop.
/// Ensures cleanup even if the switchover panics.
struct SwitchoverGuard<'a, S: Clone + Eq + std::hash::Hash> {
    state: &'a RwLock<ManagerState<S>>,
    shard_id: usize,
}

impl<S: Clone + Eq + std::hash::Hash> Drop for SwitchoverGuard<'_, S> {
    fn drop(&mut self) {
        self.state
            .write()
            .shards_in_switchover
            .remove(&self.shard_id);
    }
}

impl<H: WebSocketHandler> ShardManager<H> {
    /// Create a new shard manager
    pub fn new(config: ShardManagerConfig, handler: H) -> Self {
        // Channel for connections to request hot switchover (buffer 10 requests)
        let (switchover_tx, switchover_rx) = mpsc::channel(10);
        Self {
            handler: Arc::new(handler),
            config,
            metrics: Arc::new(Metrics::new()),
            state: Arc::new(RwLock::new(ManagerState::default())),
            shard_handles: RwLock::new(HashMap::new()),
            next_shard_id: AtomicUsize::new(0),
            lifecycle_lock: Mutex::new(()),
            switchover_tx,
            switchover_rx: Mutex::new(switchover_rx),
        }
    }

    /// Get the metrics for this manager
    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    /// Get a reference to the handler
    pub fn handler(&self) -> &Arc<H> {
        &self.handler
    }

    /// Check if the manager is currently running
    pub fn is_running(&self) -> bool {
        self.state.read().is_running
    }

    /// Start the shard manager
    ///
    /// This will create the initial shards based on existing subscriptions
    /// from the handler. If there are no initial subscriptions, no shards
    /// are created until the first subscription is added (lazy creation).
    ///
    /// # Errors
    ///
    /// Returns an error if the manager is already running or if configuration
    /// is invalid.
    pub async fn start(&self) -> Result<(), Error> {
        // Acquire lifecycle lock to serialize start/stop operations.
        // This prevents race conditions where concurrent start() calls could
        // both pass the is_running check before either sets is_running = true.
        let _lifecycle_guard = self.lifecycle_lock.lock().await;

        // Check if already running (now safe under lifecycle lock)
        {
            let state = self.state.read();
            if state.is_running {
                return Err(Error::Handler(
                    "ShardManager is already running".to_string(),
                ));
            }
        }

        let subscriptions = self.handler.subscriptions();
        let max_per_shard = self.max_per_shard();

        // Validate configuration
        if max_per_shard == 0 {
            return Err(Error::Handler(
                "max_subscriptions_per_shard cannot be 0".to_string(),
            ));
        }

        // Calculate needed shards (0 if no subscriptions - lazy creation)
        let shard_count = if subscriptions.is_empty() {
            0
        } else {
            subscriptions.len().div_ceil(max_per_shard)
        };

        info!(
            "Starting ShardManager with {} subscriptions across {} shards (max {} per shard)",
            subscriptions.len(),
            shard_count,
            max_per_shard
        );

        // Create shards using atomic counter for IDs
        // Collect the shard IDs we create for distributing subscriptions
        let mut created_shard_ids = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let shard_id = self.next_shard_id.fetch_add(1, Ordering::SeqCst);
            self.create_shard(shard_id).await?;
            created_shard_ids.push(shard_id);
        }

        // Distribute initial subscriptions to shard state
        let mut shard_subscriptions: HashMap<usize, Vec<H::Subscription>> = HashMap::new();
        for (i, sub) in subscriptions.into_iter().enumerate() {
            let shard_id = created_shard_ids[i % shard_count];
            shard_subscriptions
                .entry(shard_id)
                .or_default()
                .push(sub.clone());
            let mut state = self.state.write();
            if let Some(shard) = state.shards.get_mut(&shard_id) {
                shard.subscriptions.insert(sub.clone());
            }
            state.subscription_to_shard.insert(sub, shard_id);
        }

        // Send subscription messages to each shard
        for (shard_id, subs) in shard_subscriptions {
            if subs.is_empty() {
                continue;
            }
            let command_tx = {
                let state = self.state.read();
                state.shards.get(&shard_id).map(|s| s.command_tx.clone())
            };
            if let Some(tx) = command_tx {
                if let Some(msg) = self.handler.subscription_message(&subs) {
                    if let Err(e) = tx.send(ConnectionCommand::Send(msg)).await {
                        warn!(
                            "[SHARD-{}] Failed to send initial subscription message: {}",
                            shard_id, e
                        );
                    }
                }
            }
        }

        {
            let mut state = self.state.write();
            state.is_running = true;
        }

        // Update metrics
        let state = self.state.read();
        for (shard_id, shard) in &state.shards {
            self.metrics.update_shard(*shard_id, |s| {
                s.subscription_count = shard.subscription_count();
            });
        }

        Ok(())
    }

    /// Process pending hot switchover requests from connections.
    ///
    /// Call this periodically (e.g., in a background task) to handle
    /// data timeout triggered switchovers. Returns the number of
    /// switchovers initiated.
    pub async fn process_switchover_requests(&self) -> usize {
        let mut count = 0;
        let mut rx = self.switchover_rx.lock().await;

        // Process all pending requests
        while let Ok(shard_id) = rx.try_recv() {
            debug!("[SHARD-{}] Processing hot switchover request", shard_id);
            if let Err(e) = self.hot_switchover(shard_id).await {
                warn!("[SHARD-{}] Hot switchover failed: {}", shard_id, e);
            } else {
                count += 1;
            }
        }

        count
    }

    /// Stop all shards gracefully
    ///
    /// This will close all connections and wait for tasks to complete.
    /// After stopping, the manager can be restarted with `start()`.
    pub async fn stop(&self) -> Result<(), Error> {
        // Acquire lifecycle lock to serialize start/stop operations.
        // This prevents race conditions between concurrent start/stop calls.
        let _lifecycle_guard = self.lifecycle_lock.lock().await;

        info!("Stopping ShardManager");

        // Collect command channels while holding lock briefly
        let channels: Vec<_> = {
            let mut state = self.state.write();
            state.is_running = false;
            state
                .shards
                .values()
                .map(|s| s.command_tx.clone())
                .collect()
        };

        // Send close commands outside the lock
        for tx in channels {
            if let Err(e) = tx.send(ConnectionCommand::Close).await {
                warn!("Failed to send close command: {}", e);
            }
        }

        // Wait for all shard tasks to complete
        let handles: HashMap<usize, JoinHandle<()>> = {
            let mut handles = self.shard_handles.write();
            std::mem::take(&mut *handles)
        };

        for (_shard_id, handle) in handles {
            let _ = handle.await;
        }

        // Clean up state to prevent unbounded memory growth
        {
            let mut state = self.state.write();
            state.shards.clear();
            state.shards.shrink_to_fit();
            state.subscription_to_shard.clear();
            state.subscription_to_shard.shrink_to_fit();
            state.shards_in_switchover.clear();
            state.shards_being_created.clear();
        }

        // Reset shard ID counter for potential restart
        self.next_shard_id.store(0, Ordering::SeqCst);

        info!("ShardManager stopped");
        Ok(())
    }

    /// Subscribe to new items
    ///
    /// Returns per-item results indicating success, failure, or if already subscribed.
    /// This method continues processing remaining items even if some fail.
    ///
    /// # Returns
    ///
    /// A vector of `SubscribeResult` for each input item (in the same order).
    pub async fn subscribe(&self, items: Vec<H::Subscription>) -> Vec<SubscribeResult> {
        let mut results = Vec::with_capacity(items.len());
        let max_per_shard = self.max_per_shard();

        for item in items {
            let result = self.subscribe_single(item, max_per_shard).await;
            results.push(result);
        }

        results
    }

    /// Subscribe to a single item
    async fn subscribe_single(
        &self,
        item: H::Subscription,
        max_per_shard: usize,
    ) -> SubscribeResult {
        // Single write lock for check + add to prevent TOCTOU race
        let result = {
            let mut state = self.state.write();

            // Check if already subscribed
            if let Some(&shard_id) = state.subscription_to_shard.get(&item) {
                trace!("[SHARD-{}] Subscription already exists, skipping", shard_id);
                return SubscribeResult::AlreadySubscribed { shard_id };
            }

            // Find shard with capacity, excluding shards in switchover
            let mut last_used = state.last_shard_used;
            let existing = select_shard_excluding(
                &state.shards,
                &state.shards_in_switchover,
                self.config.shard_selection_strategy,
                &mut last_used,
            );
            state.last_shard_used = last_used;

            match existing {
                Some(shard_id) => {
                    // Add subscription atomically in the same lock
                    // Use a scope to limit the mutable borrow of state.shards
                    let add_result = {
                        let shard = match state.shards.get_mut(&shard_id) {
                            Some(s) => s,
                            None => {
                                error!("[SHARD-{}] Shard not found in HashMap", shard_id);
                                return SubscribeResult::Failed {
                                    error: format!("Shard {} not found", shard_id),
                                };
                            }
                        };
                        if !shard.add_subscription(item.clone()) {
                            error!(
                                "[SHARD-{}] Failed to add subscription - shard unexpectedly at capacity",
                                shard_id
                            );
                            return SubscribeResult::Failed {
                                error: format!(
                                    "Subscription limit exceeded: {}/{}",
                                    state.subscription_to_shard.len(),
                                    state.shards.len() * max_per_shard
                                ),
                            };
                        }
                        let tx = shard.command_tx.clone();
                        let sub_count = shard.subscription_count();
                        (tx, sub_count)
                    };
                    state.subscription_to_shard.insert(item.clone(), shard_id);
                    trace!(
                        "[SHARD-{}] Added subscription (count: {})",
                        shard_id,
                        add_result.1
                    );
                    Ok((shard_id, add_result.0, add_result.1))
                }
                None if self.config.auto_rebalance => {
                    // Need to create new shard - use atomic counter for thread-safe ID assignment
                    let new_id = self.next_shard_id.fetch_add(1, Ordering::SeqCst);
                    state.shards_being_created.insert(new_id);
                    Err(new_id)
                }
                None => {
                    return SubscribeResult::Failed {
                        error: format!(
                            "Subscription limit exceeded: {}/{}",
                            state.subscription_to_shard.len(),
                            state.shards.len() * max_per_shard
                        ),
                    };
                }
            }
        };

        let (shard_id, command_tx, sub_count) = match result {
            Ok(tuple) => tuple,
            Err(new_id) => {
                // Create new shard outside the lock
                let create_result = self.create_shard(new_id).await;

                // Clean up reservation and handle result
                let mut state = self.state.write();
                state.shards_being_created.remove(&new_id);

                if let Err(e) = create_result {
                    return SubscribeResult::Failed {
                        error: format!("Failed to create shard: {}", e),
                    };
                }
                self.metrics.record_rebalance();

                // Check if another thread already subscribed this item
                if let Some(&existing_shard) = state.subscription_to_shard.get(&item) {
                    return SubscribeResult::AlreadySubscribed {
                        shard_id: existing_shard,
                    };
                }

                // New shard should always have capacity
                // Use a scope to limit the mutable borrow of state.shards
                let add_result = {
                    let shard = match state.shards.get_mut(&new_id) {
                        Some(s) => s,
                        None => {
                            error!(
                                "[SHARD-{}] Newly created shard not found in HashMap",
                                new_id
                            );
                            return SubscribeResult::Failed {
                                error: format!("Newly created shard {} not found", new_id),
                            };
                        }
                    };
                    if !shard.add_subscription(item.clone()) {
                        error!("[SHARD-{}] New shard unexpectedly at capacity", new_id);
                        return SubscribeResult::Failed {
                            error: "New shard unexpectedly at capacity".to_string(),
                        };
                    }
                    let tx = shard.command_tx.clone();
                    let count = shard.subscription_count();
                    (tx, count)
                };
                state.subscription_to_shard.insert(item.clone(), new_id);
                trace!(
                    "[SHARD-{}] Created new shard, added subscription (count: {})",
                    new_id,
                    add_result.1
                );
                (new_id, add_result.0, add_result.1)
            }
        };

        // Update metrics BEFORE sending (to avoid potential deadlock)
        self.metrics
            .update_shard(shard_id, |s| s.subscription_count = sub_count);

        // Send subscribe message outside the lock
        if let Some(msg) = self
            .handler
            .subscription_message(std::slice::from_ref(&item))
        {
            if let Err(e) = command_tx.send(ConnectionCommand::Send(msg)).await {
                self.metrics.record_subscription_send_failed();
                warn!(
                    "[SHARD-{}] Failed to send subscription message: {}",
                    shard_id, e
                );

                // Rollback: remove subscription from state so retry is possible
                {
                    let mut state = self.state.write();
                    if let Some(shard) = state.shards.get_mut(&shard_id) {
                        shard.remove_subscription(&item);
                    }
                    state.subscription_to_shard.remove(&item);
                }

                // Update metrics to reflect rollback
                let new_count = {
                    let state = self.state.read();
                    state
                        .shards
                        .get(&shard_id)
                        .map(|s| s.subscription_count())
                        .unwrap_or(0)
                };
                self.metrics
                    .update_shard(shard_id, |s| s.subscription_count = new_count);

                return SubscribeResult::SendFailed {
                    shard_id,
                    error: e.to_string(),
                };
            }
        }

        SubscribeResult::Success { shard_id }
    }

    /// Subscribe to items and return affected shard IDs.
    ///
    /// This is a convenience method that returns the shard IDs of successful
    /// subscriptions. Use [`Self::subscribe`] instead if you need per-item error details.
    ///
    /// # Errors
    ///
    /// Returns an error only if all subscriptions fail. Partial success returns `Ok`
    /// with the shard IDs that were affected.
    pub async fn subscribe_all(&self, items: Vec<H::Subscription>) -> Result<Vec<usize>, Error> {
        let results = self.subscribe(items).await;
        let mut affected_shards_set = HashSet::new();
        let mut had_failure = false;
        let mut last_error = String::new();

        for result in results {
            match result {
                SubscribeResult::Success { shard_id } => {
                    affected_shards_set.insert(shard_id);
                }
                SubscribeResult::AlreadySubscribed { shard_id } => {
                    affected_shards_set.insert(shard_id);
                }
                SubscribeResult::SendFailed { shard_id, error } => {
                    had_failure = true;
                    last_error = error;
                    affected_shards_set.insert(shard_id);
                }
                SubscribeResult::Failed { error } => {
                    had_failure = true;
                    last_error = error;
                }
            }
        }

        if had_failure && affected_shards_set.is_empty() {
            return Err(Error::Handler(last_error));
        }

        Ok(affected_shards_set.into_iter().collect())
    }

    /// Unsubscribe from items
    ///
    /// Items not currently subscribed are silently skipped.
    pub async fn unsubscribe(&self, items: Vec<H::Subscription>) -> Result<(), Error> {
        // Use single write lock for atomic grouping and removal
        let to_send: Vec<ShardUnsubData<H::Subscription>> = {
            let mut state = self.state.write();
            let mut by_shard: HashMap<usize, Vec<H::Subscription>> = HashMap::new();

            // Group by shard and remove atomically
            for item in items {
                if let Some(&shard_id) = state.subscription_to_shard.get(&item) {
                    by_shard.entry(shard_id).or_default().push(item);
                }
            }

            let mut result = Vec::new();
            for (shard_id, subs) in by_shard {
                // Validate shard exists using HashMap lookup
                // Use a scope to limit the mutable borrow of state.shards
                let shard_result = {
                    let shard = match state.shards.get_mut(&shard_id) {
                        Some(s) => s,
                        None => {
                            warn!(
                                "[SHARD-{}] Shard no longer exists during unsubscribe",
                                shard_id
                            );
                            // Clean up orphaned subscriptions from the map
                            for sub in &subs {
                                state.subscription_to_shard.remove(sub);
                            }
                            continue;
                        }
                    };

                    // Remove subscriptions from shard
                    for sub in &subs {
                        shard.remove_subscription(sub);
                    }
                    trace!("[SHARD-{}] Removed {} subscriptions", shard_id, subs.len());

                    let tx = shard.command_tx.clone();
                    let count = shard.subscription_count();
                    (tx, count)
                };

                // Now remove from subscription_to_shard map (outside of shards borrow)
                for sub in &subs {
                    state.subscription_to_shard.remove(sub);
                }

                result.push((shard_id, subs, shard_result.0, shard_result.1));
            }

            result
        };

        // Send unsubscribe messages and update metrics outside the lock
        for (shard_id, subs, command_tx, sub_count) in to_send {
            // Update metrics first
            self.metrics
                .update_shard(shard_id, |s| s.subscription_count = sub_count);

            // Send unsubscribe message
            if let Some(msg) = self.handler.unsubscription_message(&subs) {
                if let Err(e) = command_tx.send(ConnectionCommand::Send(msg)).await {
                    self.metrics.record_subscription_send_failed();
                    warn!(
                        "[SHARD-{}] Failed to send unsubscription message: {}",
                        shard_id, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Get total subscription count
    pub fn total_subscriptions(&self) -> usize {
        self.state.read().subscription_to_shard.len()
    }

    /// Get shard count
    pub fn shard_count(&self) -> usize {
        self.state.read().shards.len()
    }

    /// Get subscriptions for a specific shard
    ///
    /// Returns `None` if the shard doesn't exist.
    pub fn shard_subscriptions(&self, shard_id: usize) -> Option<Vec<H::Subscription>> {
        let state = self.state.read();
        state
            .shards
            .get(&shard_id)
            .map(|shard| shard.subscriptions.iter().cloned().collect())
    }

    /// Get which shard a subscription is on
    ///
    /// Returns `None` if the subscription is not found.
    pub fn subscription_shard(&self, subscription: &H::Subscription) -> Option<usize> {
        self.state
            .read()
            .subscription_to_shard
            .get(subscription)
            .copied()
    }

    /// Get the time since last message for a shard.
    ///
    /// Returns `None` if the shard doesn't exist, is disconnected, or has never received data.
    /// This is a lower-level method; prefer `is_subscription_fresh` for most use cases.
    pub fn shard_freshness(&self, shard_id: usize) -> Option<Duration> {
        let metrics = self.metrics.shard_metrics();
        let shard = metrics.get(shard_id)?;

        if !shard.is_connected {
            trace!("[SHARD-{}] Freshness check: disconnected", shard_id);
            return None;
        }

        let freshness = shard.time_since_last_message;
        trace!(
            "[SHARD-{}] Freshness check: {:?}",
            shard_id,
            freshness
                .map(|d| format!("{}ms", d.as_millis()))
                .unwrap_or_else(|| "no data".to_string())
        );
        freshness
    }

    /// Check if a subscription's data is considered fresh.
    ///
    /// Returns `true` if:
    /// - The subscription exists and is assigned to a shard
    /// - The shard is currently connected
    /// - The shard has received data within `max_staleness`
    ///
    /// Use this before acting on data from a subscription to avoid stale data
    /// after WebSocket disconnections or network issues.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// // Only execute if data is less than 500ms old
    /// if manager.is_subscription_fresh(&market_id, Duration::from_millis(500)) {
    ///     execute_trade();
    /// }
    /// ```
    pub fn is_subscription_fresh(
        &self,
        subscription: &H::Subscription,
        max_staleness: Duration,
    ) -> bool {
        let Some(shard_id) = self.subscription_shard(subscription) else {
            trace!("Freshness check: subscription not found");
            return false;
        };

        match self.shard_freshness(shard_id) {
            Some(since_last_message) => {
                let is_fresh = since_last_message <= max_staleness;
                trace!(
                    "[SHARD-{}] Subscription freshness: {}ms vs max {}ms -> {}",
                    shard_id,
                    since_last_message.as_millis(),
                    max_staleness.as_millis(),
                    if is_fresh { "FRESH" } else { "STALE" }
                );
                is_fresh
            }
            None => {
                trace!(
                    "[SHARD-{}] Subscription freshness: no data -> STALE",
                    shard_id
                );
                false
            }
        }
    }

    /// Check if WebSocket connections are healthy for circuit breaker integration.
    ///
    /// Returns `(connected_count, total_count, is_healthy)` where:
    /// - `connected_count`: Number of shards currently connected
    /// - `total_count`: Total number of shards
    /// - `is_healthy`: True if at least `min_connected_ratio` of shards are connected
    ///
    /// Use this method to integrate with circuit breakers. When `is_healthy` is false,
    /// trading should be paused until connections recover.
    ///
    /// # Arguments
    ///
    /// * `min_connected_ratio` - Minimum ratio of connected shards (0.0 to 1.0).
    ///   Use 0.5 to require at least half connected, 1.0 to require all connected.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Trip circuit breaker if less than 50% of shards are connected
    /// let (connected, total, healthy) = manager.websocket_health(0.5);
    /// if !healthy && total > 0 {
    ///     circuit_breaker.trip(TripReason::WebSocketDisconnected { connected, total });
    /// }
    /// ```
    pub fn websocket_health(&self, min_connected_ratio: f64) -> (usize, usize, bool) {
        let metrics = self.metrics.shard_metrics();
        let total = metrics.len();

        if total == 0 {
            // No shards exist yet (lazy creation) - consider healthy
            return (0, 0, true);
        }

        let connected = metrics.iter().filter(|s| s.is_connected).count();
        let ratio = connected as f64 / total as f64;
        let is_healthy = ratio >= min_connected_ratio;

        (connected, total, is_healthy)
    }

    /// Force reconnection of a specific shard
    pub async fn reconnect_shard(&self, shard_id: usize) -> Result<(), Error> {
        let command_tx = {
            let state = self.state.read();
            state.shards.get(&shard_id).map(|s| s.command_tx.clone())
        };

        if let Some(tx) = command_tx {
            tx.send(ConnectionCommand::Reconnect)
                .await
                .map_err(|e| Error::ChannelSend(e.to_string()))?;
        }
        Ok(())
    }

    /// Trigger hot switchover for a shard
    ///
    /// Creates a new connection, waits for it to be ready, then closes the old one.
    /// This ensures no data loss during the transition.
    ///
    /// During hot switchover, new subscriptions to this shard are blocked and will
    /// be routed to other shards (or trigger new shard creation if auto_rebalance is enabled).
    pub async fn hot_switchover(&self, shard_id: usize) -> Result<(), Error> {
        if !self.config.hot_switchover {
            warn!("Hot switchover is disabled, performing regular reconnect");
            return self.reconnect_shard(shard_id).await;
        }

        // Check if switchover already in progress for this shard (prevent concurrent switchovers)
        {
            let mut state = self.state.write();
            if state.shards_in_switchover.contains(&shard_id) {
                info!(
                    "[SHARD-{}] Hot switchover already in progress, skipping",
                    shard_id
                );
                return Ok(());
            }
            state.shards_in_switchover.insert(shard_id);
        }

        // Create guard AFTER inserting - ensures cleanup on all exit paths including panic
        let _guard = SwitchoverGuard {
            state: &self.state,
            shard_id,
        };

        debug!("[SHARD-{}] Starting hot switchover", shard_id);
        self.metrics.record_hot_switchover();

        let result = self.do_hot_switchover(shard_id).await;

        // Guard automatically removes from switchover set on drop

        if let Err(ref e) = result {
            self.metrics.record_hot_switchover_failed();
            error!("[SHARD-{}] Hot switchover failed: {}", shard_id, e);
        } else {
            debug!("[SHARD-{}] Hot switchover complete", shard_id);
        }

        result
    }

    /// Internal hot switchover implementation
    async fn do_hot_switchover(&self, shard_id: usize) -> Result<(), Error> {
        // Get current subscriptions for this shard
        let subscriptions: Vec<H::Subscription> = {
            let state = self.state.read();
            match state.shards.get(&shard_id) {
                Some(shard) => shard.subscriptions.iter().cloned().collect(),
                None => {
                    warn!("[SHARD-{}] Shard not found for hot switchover", shard_id);
                    return Err(Error::HotSwitchoverFailed(format!(
                        "Shard {} not found",
                        shard_id
                    )));
                }
            }
        };

        info!(
            "[SHARD-{}] Hot switchover with {} subscriptions",
            shard_id,
            subscriptions.len()
        );

        // Create channels for new connection
        let (new_tx, new_rx) = mpsc::channel::<ConnectionCommand>(DEFAULT_CHANNEL_SIZE);
        let (ready_tx, ready_rx) = oneshot::channel();

        // Create new connection with ready signal, explicit subscriptions, and switchover channel
        let new_connection = Connection::with_ready_signal(
            shard_id,
            self.handler.clone(),
            self.config.connection.clone(),
            self.config.backoff.clone(),
            self.config.health.clone(),
            self.metrics.clone(),
            new_rx,
            ready_tx,
            subscriptions,
            self.switchover_tx.clone(),
        );

        // Spawn new connection task with proper logging
        let handle = tokio::spawn(async move {
            match new_connection.run().await {
                Ok(()) => {
                    debug!(
                        "[SHARD-{}] Hot switchover connection task exited normally",
                        shard_id
                    );
                }
                Err(e) => {
                    error!(
                        "[SHARD-{}] Hot switchover connection failed: {}",
                        shard_id, e
                    );
                }
            }
        });

        // Wait for new connection to signal ready (with timeout)
        info!(
            "[SHARD-{}] Waiting for new connection ready signal...",
            shard_id
        );
        match tokio::time::timeout(HOT_SWITCHOVER_TIMEOUT, ready_rx).await {
            Ok(Ok(())) => {
                info!("[SHARD-{}] New connection signaled ready", shard_id);
            }
            Ok(Err(_)) => {
                // Channel was dropped - connection failed before becoming ready
                handle.abort();
                return Err(Error::HotSwitchoverFailed(
                    "Connection dropped before becoming ready".to_string(),
                ));
            }
            Err(_) => {
                // Timeout
                handle.abort();
                return Err(Error::HotSwitchoverFailed(
                    "Timeout waiting for new connection".to_string(),
                ));
            }
        }

        // Gracefully close old connection and swap
        info!(
            "[SHARD-{}] Swapping channels (new connection ready)",
            shard_id
        );
        let old_tx = {
            let mut state = self.state.write();
            if let Some(shard) = state.shards.get_mut(&shard_id) {
                let old = shard.command_tx.clone();
                shard.command_tx = new_tx;
                info!(
                    "[SHARD-{}] Channel swapped, old channel extracted",
                    shard_id
                );
                Some(old)
            } else {
                warn!("[SHARD-{}] Shard not found during channel swap!", shard_id);
                None
            }
        };

        if let Some(tx) = old_tx {
            // Send graceful close to old connection
            info!(
                "[SHARD-{}] Sending Close to old connection channel",
                shard_id
            );
            if let Err(e) = tx.send(ConnectionCommand::Close).await {
                debug!("[SHARD-{}] Old connection already closed: {}", shard_id, e);
            }
        }

        // Fix metrics race: old connection's disconnect will set is_connected=false,
        // but the new connection is already running. Re-set to true after close is sent.
        // Use a small delay to let the old connection's metrics update complete first.
        tokio::time::sleep(Duration::from_millis(10)).await;
        self.metrics.update_shard(shard_id, |s| {
            s.is_connected = true;
        });

        // Replace handle using HashMap lookup
        {
            let mut handles = self.shard_handles.write();
            if let Some(old_handle) = handles.insert(shard_id, handle) {
                // Wait for old handle in background but with proper cleanup
                tokio::spawn(async move {
                    match tokio::time::timeout(Duration::from_secs(5), old_handle).await {
                        Ok(Ok(())) => debug!("Old connection task finished gracefully"),
                        Ok(Err(e)) => warn!("Old connection task panicked: {:?}", e),
                        Err(_) => {
                            warn!("Old connection task timed out during shutdown");
                            // The handle is dropped here which aborts it
                        }
                    }
                });
            }
        }

        Ok(())
    }

    /// Create a new shard
    async fn create_shard(&self, shard_id: usize) -> Result<(), Error> {
        let (tx, rx) = mpsc::channel::<ConnectionCommand>(DEFAULT_CHANNEL_SIZE);
        let max_per_shard = self.max_per_shard();

        debug!(
            "[SHARD-{}] Creating shard with max {} subscriptions",
            shard_id, max_per_shard
        );

        // Spawn connection task with panic recovery
        let handler = self.handler.clone();
        let connection_config = self.config.connection.clone();
        let backoff_config = self.config.backoff.clone();
        let health_config = self.config.health.clone();
        let metrics = self.metrics.clone();
        let switchover_tx = self.switchover_tx.clone();

        let handle = tokio::spawn(async move {
            Self::run_connection_with_recovery(
                shard_id,
                handler,
                connection_config,
                backoff_config,
                health_config,
                metrics,
                rx,
                switchover_tx,
            )
            .await
        });

        // Add shard to state using HashMap insert
        {
            let mut state = self.state.write();
            state
                .shards
                .insert(shard_id, Shard::new(shard_id, tx, max_per_shard));
        }

        // Store handle using HashMap insert
        {
            let mut handles = self.shard_handles.write();
            handles.insert(shard_id, handle);
        }

        // Initialize metrics
        self.metrics.update_shard(shard_id, |s| {
            s.subscription_count = 0;
            s.is_connected = false;
        });

        Ok(())
    }

    /// Run connection with panic recovery
    #[allow(clippy::too_many_arguments)]
    async fn run_connection_with_recovery(
        shard_id: usize,
        handler: Arc<H>,
        connection_config: crate::config::ConnectionConfig,
        backoff_config: crate::config::BackoffConfig,
        health_config: crate::config::HealthConfig,
        metrics: Arc<Metrics>,
        command_rx: mpsc::Receiver<ConnectionCommand>,
        switchover_tx: mpsc::Sender<usize>,
    ) {
        // Note: We can't easily restart the connection after channel closure,
        // so we just catch panics within this task and log them.
        // For full recovery, the manager would need to recreate the channel.
        let connection = Connection::with_switchover_channel(
            shard_id,
            handler,
            connection_config,
            backoff_config,
            health_config,
            metrics.clone(),
            command_rx,
            switchover_tx,
        );

        match AssertUnwindSafe(connection.run()).catch_unwind().await {
            Ok(Ok(())) => {
                debug!("[SHARD-{}] Connection task completed normally", shard_id);
            }
            Ok(Err(e)) => {
                warn!(
                    "[SHARD-{}] Connection task ended with error: {}",
                    shard_id, e
                );
            }
            Err(panic_err) => {
                // Extract panic message if possible
                let panic_msg = if let Some(s) = panic_err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = panic_err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown panic".to_string()
                };
                error!(
                    "[SHARD-{}] Connection task PANICKED: {}. Shard is now dead.",
                    shard_id, panic_msg
                );
                metrics.record_error();
            }
        }
    }

    /// Get max subscriptions per shard from config or handler
    fn max_per_shard(&self) -> usize {
        self.config
            .max_subscriptions_per_shard
            .unwrap_or_else(|| self.handler.max_subscriptions_per_shard())
    }
}

impl<H: WebSocketHandler> Drop for ShardManager<H> {
    fn drop(&mut self) {
        // Abort all shard handles to prevent orphaned tasks
        let handles = std::mem::take(&mut *self.shard_handles.write());
        for (_shard_id, handle) in handles {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::WebSocketHandler;
    use tokio_tungstenite::tungstenite::Message;

    // Minimal handler for testing
    struct TestHandler;

    impl WebSocketHandler for TestHandler {
        type Subscription = String;

        async fn url(&self, _state: &crate::handler::ConnectionState) -> String {
            "wss://example.com".to_string()
        }

        async fn on_connect(&self, _state: &crate::handler::ConnectionState) -> Vec<Message> {
            vec![]
        }

        async fn on_message(&self, _message: Message, _state: &crate::handler::ConnectionState) {}

        fn subscriptions(&self) -> Vec<Self::Subscription> {
            vec![]
        }

        fn subscription_message(&self, _subs: &[Self::Subscription]) -> Option<Message> {
            None
        }
    }

    #[test]
    fn test_shard_freshness_no_shard() {
        let config = ShardManagerConfig::default();
        let manager = ShardManager::new(config, TestHandler);

        // No shards exist yet
        assert!(manager.shard_freshness(0).is_none());
    }

    #[test]
    fn test_is_subscription_fresh_not_subscribed() {
        let config = ShardManagerConfig::default();
        let manager = ShardManager::new(config, TestHandler);

        // Subscription doesn't exist
        assert!(!manager.is_subscription_fresh(&"unknown".to_string(), Duration::from_secs(1)));
    }

    #[test]
    fn test_shard_freshness_disconnected() {
        let config = ShardManagerConfig::default();
        let manager = ShardManager::new(config, TestHandler);

        // Simulate a disconnected shard
        manager.metrics.update_shard(0, |s| {
            s.is_connected = false;
            s.last_message_at = Some(std::time::Instant::now());
        });

        // Should return None because disconnected
        assert!(manager.shard_freshness(0).is_none());
    }

    #[test]
    fn test_shard_freshness_connected_with_data() {
        let config = ShardManagerConfig::default();
        let manager = ShardManager::new(config, TestHandler);

        // Simulate a connected shard with recent data
        manager.metrics.update_shard(0, |s| {
            s.is_connected = true;
        });
        // Use the atomic method to record message received (this is the actual API)
        manager.metrics.record_shard_message_received(0);

        // Should return Some duration (very small since we just set it)
        let freshness = manager.shard_freshness(0);
        assert!(freshness.is_some());
        assert!(freshness.unwrap() < Duration::from_secs(1));
    }

    #[test]
    fn test_websocket_health_no_shards() {
        let config = ShardManagerConfig::default();
        let manager = ShardManager::new(config, TestHandler);

        // No shards exist yet (lazy creation) - should be considered healthy
        let (connected, total, is_healthy) = manager.websocket_health(0.5);
        assert_eq!(connected, 0);
        assert_eq!(total, 0);
        assert!(is_healthy);
    }

    #[test]
    fn test_websocket_health_all_connected() {
        let config = ShardManagerConfig::default();
        let manager = ShardManager::new(config, TestHandler);

        // Simulate 3 connected shards
        for i in 0..3 {
            manager.metrics.update_shard(i, |s| {
                s.is_connected = true;
            });
        }

        let (connected, total, is_healthy) = manager.websocket_health(0.5);
        assert_eq!(connected, 3);
        assert_eq!(total, 3);
        assert!(is_healthy);
    }

    #[test]
    fn test_websocket_health_partial_connected() {
        let config = ShardManagerConfig::default();
        let manager = ShardManager::new(config, TestHandler);

        // Simulate 4 shards, only 2 connected
        for i in 0..4 {
            manager.metrics.update_shard(i, |s| {
                s.is_connected = i < 2; // First 2 connected
            });
        }

        // 50% connected should be healthy with 0.5 threshold
        let (connected, total, is_healthy) = manager.websocket_health(0.5);
        assert_eq!(connected, 2);
        assert_eq!(total, 4);
        assert!(is_healthy);

        // But not healthy with 0.75 threshold
        let (_, _, is_healthy_75) = manager.websocket_health(0.75);
        assert!(!is_healthy_75);
    }

    #[test]
    fn test_websocket_health_all_disconnected() {
        let config = ShardManagerConfig::default();
        let manager = ShardManager::new(config, TestHandler);

        // Simulate 3 disconnected shards
        for i in 0..3 {
            manager.metrics.update_shard(i, |s| {
                s.is_connected = false;
            });
        }

        let (connected, total, is_healthy) = manager.websocket_health(0.5);
        assert_eq!(connected, 0);
        assert_eq!(total, 3);
        assert!(!is_healthy);
    }
}
