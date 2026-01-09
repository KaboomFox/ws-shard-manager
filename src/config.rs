use crate::shard::ShardSelectionStrategy;
use std::time::Duration;

/// Configuration for the shard manager
#[derive(Debug, Clone)]
pub struct ShardManagerConfig {
    /// Connection-related settings
    pub connection: ConnectionConfig,
    /// Backoff settings for reconnection
    pub backoff: BackoffConfig,
    /// Health monitoring settings
    pub health: HealthConfig,
    /// Maximum subscriptions per shard (overrides handler default)
    pub max_subscriptions_per_shard: Option<usize>,
    /// Enable hot switchover (connect new before disconnecting old)
    pub hot_switchover: bool,
    /// Enable auto-rebalancing when shards exceed capacity
    pub auto_rebalance: bool,
    /// Strategy for selecting shards for new subscriptions
    pub shard_selection_strategy: ShardSelectionStrategy,
}

impl Default for ShardManagerConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            backoff: BackoffConfig::default(),
            health: HealthConfig::default(),
            max_subscriptions_per_shard: None,
            hot_switchover: true,
            auto_rebalance: true,
            shard_selection_strategy: ShardSelectionStrategy::default(),
        }
    }
}

impl ShardManagerConfig {
    /// Create a new builder for configuration
    pub fn builder() -> ShardManagerConfigBuilder {
        ShardManagerConfigBuilder::default()
    }
}

/// Builder for ShardManagerConfig
#[derive(Debug, Clone, Default)]
pub struct ShardManagerConfigBuilder {
    config: ShardManagerConfig,
}

impl ShardManagerConfigBuilder {
    /// Set connection configuration
    pub fn connection(mut self, config: ConnectionConfig) -> Self {
        self.config.connection = config;
        self
    }

    /// Set backoff configuration
    pub fn backoff(mut self, config: BackoffConfig) -> Self {
        self.config.backoff = config;
        self
    }

    /// Set health configuration
    pub fn health(mut self, config: HealthConfig) -> Self {
        self.config.health = config;
        self
    }

    /// Set maximum subscriptions per shard
    pub fn max_subscriptions_per_shard(mut self, max: usize) -> Self {
        self.config.max_subscriptions_per_shard = Some(max);
        self
    }

    /// Enable or disable hot switchover
    pub fn hot_switchover(mut self, enabled: bool) -> Self {
        self.config.hot_switchover = enabled;
        self
    }

    /// Enable or disable auto-rebalancing
    pub fn auto_rebalance(mut self, enabled: bool) -> Self {
        self.config.auto_rebalance = enabled;
        self
    }

    /// Set the shard selection strategy
    pub fn shard_selection_strategy(mut self, strategy: ShardSelectionStrategy) -> Self {
        self.config.shard_selection_strategy = strategy;
        self
    }

    /// Build the configuration with validation.
    ///
    /// Returns an error for invalid configurations (e.g., max_subscriptions_per_shard = 0).
    pub fn build(self) -> Result<ShardManagerConfig, ConfigError> {
        // Validate backoff config
        if self.config.backoff.max_delay < self.config.backoff.initial_delay {
            return Err(ConfigError::InvalidBackoff(
                "max_delay must be >= initial_delay".to_string(),
            ));
        }

        if self.config.backoff.multiplier <= 0.0 {
            return Err(ConfigError::InvalidBackoff(
                "multiplier must be > 0".to_string(),
            ));
        }

        // Validate health config
        if self.config.health.pong_timeout > self.config.health.ping_interval {
            return Err(ConfigError::InvalidHealth(
                "pong_timeout should be <= ping_interval".to_string(),
            ));
        }

        // Validate max subscriptions
        if let Some(0) = self.config.max_subscriptions_per_shard {
            return Err(ConfigError::InvalidSubscriptionLimit(
                "max_subscriptions_per_shard cannot be 0".to_string(),
            ));
        }

        Ok(self.config)
    }
}

/// Configuration validation errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConfigError {
    /// Invalid backoff configuration
    #[error("Invalid backoff configuration: {0}")]
    InvalidBackoff(String),
    /// Invalid health configuration
    #[error("Invalid health configuration: {0}")]
    InvalidHealth(String),
    /// Invalid subscription limit
    #[error("Invalid subscription limit: {0}")]
    InvalidSubscriptionLimit(String),
}

/// Connection-related configuration
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Timeout for establishing a connection
    pub connect_timeout: Duration,
    /// Maximum number of connection attempts before giving up
    pub max_connect_attempts: u32,
    /// Proactive reconnection interval (to prevent stale connections)
    pub proactive_reconnect_interval: Option<Duration>,
    /// Circuit breaker: consecutive failures before tripping
    pub circuit_breaker_threshold: u32,
    /// Circuit breaker: how long to wait before allowing retry after trip
    pub circuit_breaker_reset_timeout: Duration,
    /// Low-latency mode: disable panic protection for message handlers.
    /// When enabled, handler panics will crash the connection task.
    /// Enable this for trading bots where microseconds matter.
    pub low_latency_mode: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            max_connect_attempts: 10,
            proactive_reconnect_interval: Some(Duration::from_secs(15 * 60)), // 15 minutes
            circuit_breaker_threshold: 5, // Trip after 5 consecutive failures
            circuit_breaker_reset_timeout: Duration::from_secs(60), // Wait 60s before retry
            low_latency_mode: false, // Safe by default
        }
    }
}

/// Backoff configuration for reconnection
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial delay before first reconnection attempt
    pub initial_delay: Duration,
    /// Maximum delay between reconnection attempts
    pub max_delay: Duration,
    /// Multiplier for exponential backoff (typically 2.0)
    pub multiplier: f64,
    /// Whether to add random jitter to delays
    pub jitter: bool,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true, // Full jitter recommended by AWS
        }
    }
}

impl BackoffConfig {
    /// Calculate the delay for a given attempt number (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_delay = self.initial_delay.as_millis() as f64
            * self.multiplier.powi(attempt as i32);
        let capped_delay = base_delay.min(self.max_delay.as_millis() as f64);

        if self.jitter {
            // Full jitter: random value between 0 and capped_delay
            let jittered = rand::random::<f64>() * capped_delay;
            Duration::from_millis(jittered as u64)
        } else {
            Duration::from_millis(capped_delay as u64)
        }
    }
}

/// Health monitoring configuration
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Interval for sending WebSocket pings
    pub ping_interval: Duration,
    /// Timeout for receiving a pong response
    pub pong_timeout: Duration,
    /// Timeout for receiving any data (message or heartbeat)
    pub data_timeout: Duration,
    /// Number of consecutive failures before marking unhealthy
    pub failure_threshold: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(20),
            pong_timeout: Duration::from_secs(10),
            data_timeout: Duration::from_secs(30),
            failure_threshold: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_delay_calculation() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: false,
        };

        assert_eq!(config.delay_for_attempt(0), Duration::from_millis(100));
        assert_eq!(config.delay_for_attempt(1), Duration::from_millis(200));
        assert_eq!(config.delay_for_attempt(2), Duration::from_millis(400));
        assert_eq!(config.delay_for_attempt(3), Duration::from_millis(800));

        // Should cap at max_delay
        assert_eq!(config.delay_for_attempt(10), Duration::from_secs(30));
    }

    #[test]
    fn test_backoff_with_jitter() {
        let config = BackoffConfig {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        };

        // With jitter, delay should be between 0 and the calculated delay
        for attempt in 0..5 {
            let delay = config.delay_for_attempt(attempt);
            let max_expected = Duration::from_millis(
                (100.0 * 2.0_f64.powi(attempt as i32)) as u64
            );
            assert!(delay <= max_expected);
        }
    }

    #[test]
    fn test_config_builder() {
        let config = ShardManagerConfig::builder()
            .max_subscriptions_per_shard(100)
            .hot_switchover(false)
            .build()
            .expect("valid config");

        assert_eq!(config.max_subscriptions_per_shard, Some(100));
        assert!(!config.hot_switchover);
        assert!(config.auto_rebalance); // default
    }

    #[test]
    fn test_config_builder_rejects_zero_subscriptions() {
        let result = ShardManagerConfig::builder()
            .max_subscriptions_per_shard(0)
            .build();

        assert!(result.is_err());
    }
}
