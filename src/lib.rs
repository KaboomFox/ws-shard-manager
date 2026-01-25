//! # ws-shard-manager
//!
//! A robust WebSocket connection manager with sharding, auto-reconnection, and hot switchover.
//!
//! ## Features
//!
//! - **Auto-reconnection** with exponential backoff and full jitter
//! - **Sharding** for connections with subscription limits
//! - **Hot switchover** - new connection established before old one closes
//! - **Auto-rebalancing** when subscriptions exceed capacity
//! - **Health monitoring** via ping/pong and data timeouts
//! - **Metrics** for observability
//!
//! ## Example
//!
//! ```ignore
//! use ws_shard_manager::{ShardManager, ShardManagerConfig, WebSocketHandler};
//!
//! struct MyHandler;
//!
//! impl WebSocketHandler for MyHandler {
//!     // ... implement required methods
//! }
//!
//! let config = ShardManagerConfig::builder()
//!     .max_subscriptions_per_shard(500)
//!     .build()?;
//!
//! let manager = ShardManager::new(config, MyHandler);
//! manager.subscribe(vec!["BTC", "ETH"]).await?;
//! ```

mod config;
mod connection;
mod error;
mod handler;
mod health;
mod manager;
mod metrics;
mod shard;

pub use config::{BackoffConfig, ConfigError, ConnectionConfig, HealthConfig, ShardManagerConfig};
pub use error::{Error, ErrorKind, SubscribeResult};
pub use handler::{ConnectionState, SequenceRecovery, SequenceValidation, WebSocketHandler};
pub use manager::ShardManager;
pub use metrics::{Metrics, MetricsSnapshot, ShardMetrics};
pub use shard::ShardSelectionStrategy;

// Re-export http types for connection_headers
pub use http::{HeaderName, HeaderValue};

/// Result type for ws-shard-manager operations
pub type Result<T> = std::result::Result<T, Error>;
