use crate::error::ErrorKind;
use http::{HeaderName, HeaderValue};
use std::future::Future;
use tokio_tungstenite::tungstenite::Message;

// =============================================================================
// Sequence Validation Types
// =============================================================================

/// Action to take when sequence validation fails.
///
/// Used by handlers that implement sequence number tracking to specify
/// how the connection should recover from detected gaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SequenceRecovery {
    /// Continue processing the message despite the gap.
    /// Use when gaps are acceptable or for logging-only validation.
    #[default]
    Continue,

    /// Drop this message but keep the connection alive.
    /// Use when the message is stale but other subscriptions are fine.
    DropMessage,

    /// Trigger a full reconnection to get fresh snapshots.
    /// Use when the orderbook state is too corrupted to recover incrementally.
    Reconnect,
}

/// Result of sequence validation for a message.
///
/// Returned by [`WebSocketHandler::validate_sequence`] to indicate whether
/// a message's sequence number is valid and what action to take if not.
#[derive(Debug, Clone)]
pub struct SequenceValidation {
    /// Whether the sequence number is valid (no gap detected).
    pub is_valid: bool,

    /// Recovery action to take if validation failed.
    pub recovery: SequenceRecovery,

    /// Subscription identifiers that need resubscription due to sequence gaps.
    ///
    /// When non-empty, the connection will call [`WebSocketHandler::resubscription_message`]
    /// to send a resubscription request, which typically triggers fresh snapshots.
    pub resubscribe: Vec<String>,
}

impl Default for SequenceValidation {
    fn default() -> Self {
        Self {
            is_valid: true,
            recovery: SequenceRecovery::Continue,
            resubscribe: vec![],
        }
    }
}

impl SequenceValidation {
    /// Create a valid sequence result.
    #[inline]
    pub fn valid() -> Self {
        Self::default()
    }

    /// Create an invalid sequence result with the specified recovery action.
    #[inline]
    pub fn invalid(recovery: SequenceRecovery) -> Self {
        Self {
            is_valid: false,
            recovery,
            resubscribe: vec![],
        }
    }

    /// Create an invalid sequence result that triggers resubscription.
    #[inline]
    pub fn resubscribe(items: Vec<String>) -> Self {
        Self {
            is_valid: false,
            recovery: SequenceRecovery::DropMessage,
            resubscribe: items,
        }
    }
}

// =============================================================================
// Connection State
// =============================================================================

/// Connection state information passed to handlers
#[derive(Debug, Clone)]
pub struct ConnectionState {
    /// Unique identifier for this shard (0-indexed)
    pub shard_id: usize,
    /// Number of active subscriptions on this shard
    pub subscription_count: usize,
    /// Whether this is a reconnection (vs initial connection)
    pub is_reconnect: bool,
    /// Number of reconnection attempts (0 for initial connection)
    pub reconnect_attempt: u32,
}

/// Trait that users implement to handle WebSocket events.
///
/// This trait defines the contract between the shard manager and user code.
/// The manager handles connection lifecycle, reconnection, and health monitoring,
/// while the handler processes messages and manages subscriptions.
///
/// # Example
///
/// ```ignore
/// use ws_shard_manager::{WebSocketHandler, ConnectionState};
/// use tokio_tungstenite::tungstenite::Message;
///
/// struct PolymarketHandler {
///     subscriptions: Vec<String>,
/// }
///
/// impl WebSocketHandler for PolymarketHandler {
///     type Subscription = String;
///
///     async fn url(&self, state: &ConnectionState) -> String {
///         "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
///     }
///
///     async fn on_connect(&self, state: &ConnectionState) -> Vec<Message> {
///         // Return messages to send after connection
///         vec![Message::Text(r#"{"type":"subscribe","markets":["..."]}]"#.to_string())]
///     }
///
///     async fn on_message(&self, message: Message, state: &ConnectionState) {
///         // Process incoming messages
///     }
///
///     fn subscriptions(&self) -> Vec<Self::Subscription> {
///         self.subscriptions.clone()
///     }
///
///     fn subscription_message(&self, subs: &[Self::Subscription]) -> Option<Message> {
///         Some(Message::Text(format!(r#"{{"assets_ids":{:?}}}"#, subs)))
///     }
/// }
/// ```
pub trait WebSocketHandler: Send + Sync + 'static {
    /// The type representing a single subscription (e.g., token ID, market ID)
    type Subscription: Clone + Send + Sync + Eq + std::hash::Hash + std::fmt::Debug + 'static;

    /// Returns the WebSocket URL to connect to.
    ///
    /// Called before each connection attempt, allowing dynamic URL generation
    /// (e.g., including auth tokens or shard-specific endpoints).
    fn url(&self, state: &ConnectionState) -> impl Future<Output = String> + Send;

    /// Called after a successful connection.
    ///
    /// Returns a list of messages to send immediately after connecting
    /// (e.g., authentication, initial subscriptions).
    fn on_connect(&self, state: &ConnectionState) -> impl Future<Output = Vec<Message>> + Send;

    /// Called when a message is received.
    ///
    /// This is where you process incoming data from the WebSocket.
    fn on_message(
        &self,
        message: Message,
        state: &ConnectionState,
    ) -> impl Future<Output = ()> + Send;

    /// Called when the connection is closed.
    ///
    /// This is called before reconnection attempts begin.
    fn on_disconnect(&self, _state: &ConnectionState) -> impl Future<Output = ()> + Send {
        async {}
    }

    /// Called when an error occurs.
    ///
    /// Receives the error kind for type-based decision making and the error message.
    /// Return `true` to attempt reconnection, `false` to stop.
    fn on_error(
        &self,
        _kind: ErrorKind,
        _message: &str,
        _state: &ConnectionState,
    ) -> impl Future<Output = bool> + Send {
        async { true }
    }

    /// Returns all current subscriptions for this handler.
    ///
    /// Used by the shard manager to track and redistribute subscriptions.
    fn subscriptions(&self) -> Vec<Self::Subscription>;

    /// Creates a subscription message for the given subscriptions.
    ///
    /// Returns `None` if no message should be sent (e.g., empty list).
    fn subscription_message(&self, subscriptions: &[Self::Subscription]) -> Option<Message>;

    /// Creates an unsubscription message for the given subscriptions.
    ///
    /// Returns `None` if no message should be sent.
    fn unsubscription_message(&self, _subscriptions: &[Self::Subscription]) -> Option<Message> {
        None
    }

    /// Returns the maximum number of subscriptions per shard.
    ///
    /// This is used to determine when to create new shards.
    /// Defaults to 500 (Polymarket limit).
    fn max_subscriptions_per_shard(&self) -> usize {
        500
    }

    /// Returns `true` if the message indicates a valid application-level heartbeat.
    ///
    /// Some protocols send their own heartbeat messages beyond WebSocket pings.
    /// Override this to detect those and reset the data timeout.
    fn is_heartbeat(&self, _message: &Message) -> bool {
        false
    }

    // =========================================================================
    // Sequence Validation (Optional)
    // =========================================================================

    /// Validate message sequence number before processing.
    ///
    /// Called before [`on_message`](Self::on_message) for every data message
    /// (Text and Binary). Implementations can:
    ///
    /// - Track sequence numbers per subscription
    /// - Detect gaps (missed messages)
    /// - Request recovery actions (drop, reconnect, resubscribe)
    ///
    /// # Example
    ///
    /// ```ignore
    /// fn validate_sequence(&self, message: &Message, _: &ConnectionState) -> SequenceValidation {
    ///     let Message::Text(text) = message else {
    ///         return SequenceValidation::valid();
    ///     };
    ///
    ///     // Parse seq/sid from message and check against expected
    ///     let (sid, seq) = parse_sequence(text);
    ///     if self.tracker.check(sid, seq) {
    ///         SequenceValidation::valid()
    ///     } else {
    ///         SequenceValidation::resubscribe(vec![ticker])
    ///     }
    /// }
    /// ```
    ///
    /// Default implementation accepts all messages (no sequence validation).
    fn validate_sequence(
        &self,
        _message: &Message,
        _state: &ConnectionState,
    ) -> SequenceValidation {
        SequenceValidation::valid()
    }

    /// Reset sequence tracking state on new connection.
    ///
    /// Called after a successful connection is established, before sending
    /// initial messages. Implementations should clear their sequence state
    /// here since a new connection means fresh snapshots will be received.
    ///
    /// Default implementation does nothing.
    fn reset_sequence_state(&self, _state: &ConnectionState) {}

    /// Create a resubscription message for items that had sequence gaps.
    ///
    /// Called when [`validate_sequence`](Self::validate_sequence) returns
    /// [`SequenceValidation::resubscribe`] with non-empty items. The returned
    /// message is sent to trigger fresh snapshots for the affected subscriptions.
    ///
    /// Default implementation delegates to [`subscription_message`](Self::subscription_message)
    /// by converting String items. Override if your protocol has a different
    /// resubscription mechanism or if the item types don't match.
    ///
    /// Returns `None` if no message should be sent.
    fn resubscription_message(&self, _items: &[String]) -> Option<Message> {
        // Default: no resubscription message
        // Implementations should override if they use sequence validation
        None
    }

    // =========================================================================
    // Connection Headers (Optional)
    // =========================================================================

    /// Returns additional headers to include in the WebSocket connection request.
    ///
    /// Called just before establishing a connection. Use for authentication
    /// headers, API keys, or other connection-time configuration.
    ///
    /// This method is async to allow computing dynamic values like signatures
    /// that depend on the current timestamp.
    ///
    /// # Example
    ///
    /// ```ignore
    /// async fn connection_headers(&self, _: &ConnectionState) -> Vec<(HeaderName, HeaderValue)> {
    ///     let timestamp = current_timestamp_ms();
    ///     let signature = sign(&format!("{}GET/ws", timestamp));
    ///
    ///     vec![
    ///         (HeaderName::from_static("x-api-key"), HeaderValue::from_str(&self.api_key).unwrap()),
    ///         (HeaderName::from_static("x-signature"), HeaderValue::from_str(&signature).unwrap()),
    ///         (HeaderName::from_static("x-timestamp"), HeaderValue::from_str(&timestamp).unwrap()),
    ///     ]
    /// }
    /// ```
    ///
    /// Default returns empty (no additional headers).
    fn connection_headers(
        &self,
        _state: &ConnectionState,
    ) -> impl Future<Output = Vec<(HeaderName, HeaderValue)>> + Send {
        async { vec![] }
    }
}
