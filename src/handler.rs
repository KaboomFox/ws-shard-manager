use crate::error::ErrorKind;
use std::future::Future;
use tokio_tungstenite::tungstenite::Message;

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
    fn on_message(&self, message: Message, state: &ConnectionState) -> impl Future<Output = ()> + Send;

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
}
