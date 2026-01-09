use thiserror::Error;

/// Categorizes errors for handler decision-making.
///
/// This is a lightweight, cloneable representation of the error type
/// that can be passed to handler callbacks for error-type-based decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// WebSocket protocol error
    WebSocket,
    /// Connection failed (timeout, refused, etc.)
    ConnectionFailed,
    /// Health check failed
    HealthCheckFailed,
    /// Manager is shutting down
    ShuttingDown,
    /// Circuit breaker tripped
    CircuitBreakerOpen,
    /// Handler-specific error
    Handler,
    /// Other error
    Other,
}

/// Errors that can occur in ws-shard-manager
#[derive(Error, Debug)]
pub enum Error {
    /// WebSocket connection error
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// Connection failed after all retry attempts
    #[error("Connection failed after {attempts} attempts: {last_error}")]
    ConnectionFailed {
        attempts: u32,
        last_error: String,
    },

    /// Health check failed
    #[error("Health check failed: {0}")]
    HealthCheckFailed(String),

    /// Manager is shutting down
    #[error("Manager is shutting down")]
    ShuttingDown,

    /// Subscription limit exceeded
    #[error("Subscription limit exceeded: {current}/{max}")]
    SubscriptionLimitExceeded {
        current: usize,
        max: usize,
    },

    /// Handler error (user-defined)
    #[error("Handler error: {0}")]
    Handler(String),

    /// Channel send error
    #[error("Channel send error: {0}")]
    ChannelSend(String),

    /// Hot switchover failed
    #[error("Hot switchover failed: {0}")]
    HotSwitchoverFailed(String),

    /// Circuit breaker open - too many consecutive failures
    #[error("Circuit breaker open after {failures} consecutive failures")]
    CircuitBreakerOpen { failures: u32 },
}

impl Error {
    /// Get the kind of this error for decision-making.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Error::WebSocket(_) => ErrorKind::WebSocket,
            Error::ConnectionFailed { .. } => ErrorKind::ConnectionFailed,
            Error::HealthCheckFailed(_) => ErrorKind::HealthCheckFailed,
            Error::ShuttingDown => ErrorKind::ShuttingDown,
            Error::CircuitBreakerOpen { .. } => ErrorKind::CircuitBreakerOpen,
            Error::Handler(_) => ErrorKind::Handler,
            Error::SubscriptionLimitExceeded { .. } | Error::ChannelSend(_) | Error::HotSwitchoverFailed(_) => ErrorKind::Other,
        }
    }
}

/// Result of subscribing to a single item
#[derive(Debug, Clone)]
pub enum SubscribeResult {
    /// Successfully subscribed and message sent
    Success { shard_id: usize },
    /// Already subscribed (no action taken)
    AlreadySubscribed { shard_id: usize },
    /// Added to state but message send failed (may need retry)
    SendFailed { shard_id: usize, error: String },
    /// Failed to subscribe
    Failed { error: String },
}
