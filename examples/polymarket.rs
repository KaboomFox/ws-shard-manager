//! Example: Using ws-shard-manager with Polymarket
//!
//! This example demonstrates how to implement a WebSocketHandler for
//! Polymarket's price feed WebSocket.
//!
//! Run with: cargo run --example polymarket

use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{info, Level};
use ws_shard_manager::{ConnectionState, ShardManager, ShardManagerConfig, WebSocketHandler};

/// Polymarket WebSocket handler
struct PolymarketHandler {
    /// Token IDs to subscribe to
    tokens: Vec<String>,
}

impl PolymarketHandler {
    fn new(tokens: Vec<String>) -> Self {
        Self { tokens }
    }
}

impl WebSocketHandler for PolymarketHandler {
    type Subscription = String;

    async fn url(&self, state: &ConnectionState) -> String {
        // Polymarket uses a single WebSocket endpoint
        info!("[SHARD-{}] Generating URL", state.shard_id);
        "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string()
    }

    async fn on_connect(&self, state: &ConnectionState) -> Vec<Message> {
        info!(
            "[SHARD-{}] Connected! Subscriptions: {}",
            state.shard_id, state.subscription_count
        );

        // Polymarket expects a subscription message on connect
        // In a real implementation, you'd only subscribe to tokens assigned to this shard
        vec![]
    }

    async fn on_message(&self, message: Message, state: &ConnectionState) {
        match message {
            Message::Text(text) => {
                // Parse and process the message
                info!(
                    "[SHARD-{}] Received: {}",
                    state.shard_id,
                    &text[..text.len().min(100)]
                );
            }
            Message::Binary(data) => {
                info!(
                    "[SHARD-{}] Received binary: {} bytes",
                    state.shard_id,
                    data.len()
                );
            }
            _ => {}
        }
    }

    async fn on_disconnect(&self, state: &ConnectionState) {
        info!(
            "[SHARD-{}] Disconnected (reconnect: {})",
            state.shard_id, state.is_reconnect
        );
    }

    fn subscriptions(&self) -> Vec<Self::Subscription> {
        self.tokens.clone()
    }

    fn subscription_message(&self, subscriptions: &[Self::Subscription]) -> Option<Message> {
        if subscriptions.is_empty() {
            return None;
        }

        // Polymarket subscription format
        let msg = serde_json::json!({
            "assets_ids": subscriptions,
            "type": "market"
        });

        Some(Message::Text(msg.to_string()))
    }

    fn unsubscription_message(&self, subscriptions: &[Self::Subscription]) -> Option<Message> {
        if subscriptions.is_empty() {
            return None;
        }

        // Polymarket doesn't have explicit unsubscribe, but you might implement it
        None
    }

    fn max_subscriptions_per_shard(&self) -> usize {
        500 // Polymarket's limit
    }

    fn is_heartbeat(&self, message: &Message) -> bool {
        // Polymarket sends heartbeat messages
        if let Message::Text(text) = message {
            text.contains("\"type\":\"heartbeat\"")
        } else {
            false
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    // Example tokens (in a real app, these would come from your data source)
    let tokens: Vec<String> = (0..50).map(|i| format!("token-{}", i)).collect();

    info!(
        "Starting Polymarket WebSocket manager with {} tokens",
        tokens.len()
    );

    // Create handler
    let handler = PolymarketHandler::new(tokens);

    // Configure the manager
    let config = ShardManagerConfig::builder()
        .max_subscriptions_per_shard(500)
        .hot_switchover(true)
        .auto_rebalance(true)
        .build()?;

    // Create and start the manager
    let manager = Arc::new(ShardManager::new(config, handler));

    manager.start().await?;

    info!("Manager started with {} shards", manager.shard_count());

    // Get metrics
    let metrics = manager.metrics();
    info!("Initial metrics: {:?}", metrics.snapshot());

    // Subscribe to more tokens dynamically
    let new_tokens: Vec<String> = (50..60).map(|i| format!("token-{}", i)).collect();

    manager.subscribe_all(new_tokens).await?;
    info!(
        "Subscribed to 10 more tokens, total: {}",
        manager.total_subscriptions()
    );

    // Run for a while
    info!("Running... Press Ctrl+C to stop");
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    // Print final metrics
    let snapshot = metrics.snapshot();
    info!("Final metrics:");
    info!("  Connections: {}", snapshot.connections_total);
    info!("  Reconnections: {}", snapshot.reconnections_total);
    info!("  Messages received: {}", snapshot.messages_received_total);
    info!("  Active connections: {}", snapshot.active_connections);

    // Graceful shutdown
    manager.stop().await?;
    info!("Manager stopped");

    Ok(())
}
