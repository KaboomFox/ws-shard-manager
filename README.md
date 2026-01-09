# ws-shard-manager

A robust WebSocket connection manager for Rust with sharding, auto-reconnection, and hot switchover.

## Features

- **Auto-reconnection** with exponential backoff and full jitter
- **Sharding** for connections with subscription limits
- **Hot switchover** - new connection established before old one closes
- **Auto-rebalancing** when subscriptions exceed capacity
- **Health monitoring** via ping/pong and data timeouts
- **Metrics** for observability
- **Panic recovery** - handler panics are caught and logged

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ws-shard-manager = "0.1"
```

## Quick Start

```rust
use ws_shard_manager::{ShardManager, ShardManagerConfig, WebSocketHandler, ConnectionState};
use tokio_tungstenite::tungstenite::Message;

struct MyHandler {
    subscriptions: Vec<String>,
}

impl WebSocketHandler for MyHandler {
    type Subscription = String;

    async fn url(&self, _state: &ConnectionState) -> String {
        "wss://example.com/ws".to_string()
    }

    async fn on_connect(&self, state: &ConnectionState) -> Vec<Message> {
        println!("Connected to shard {}", state.shard_id);
        vec![]
    }

    async fn on_message(&self, message: Message, state: &ConnectionState) {
        println!("[Shard {}] Received: {:?}", state.shard_id, message);
    }

    fn subscriptions(&self) -> Vec<Self::Subscription> {
        self.subscriptions.clone()
    }

    fn subscription_message(&self, subs: &[Self::Subscription]) -> Option<Message> {
        Some(Message::Text(format!(r#"{{"subscribe":{:?}}}"#, subs)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let handler = MyHandler {
        subscriptions: vec!["topic1".into(), "topic2".into()],
    };

    let config = ShardManagerConfig::builder()
        .max_subscriptions_per_shard(500)
        .hot_switchover(true)
        .auto_rebalance(true)
        .build();

    let manager = ShardManager::new(config, handler);
    manager.start().await?;

    // Subscribe to more topics dynamically
    manager.subscribe(vec!["topic3".into()]).await?;

    // Get metrics
    let metrics = manager.metrics();
    println!("Active connections: {}", metrics.active_connections());

    // Graceful shutdown
    manager.stop().await?;
    Ok(())
}
```

## Configuration

```rust
use ws_shard_manager::{
    ShardManagerConfig, ConnectionConfig, BackoffConfig, HealthConfig,
    ShardSelectionStrategy,
};
use std::time::Duration;

let config = ShardManagerConfig::builder()
    // Connection settings
    .connection(ConnectionConfig {
        connect_timeout: Duration::from_secs(10),
        max_connect_attempts: 10,
        proactive_reconnect_interval: Some(Duration::from_secs(15 * 60)),
    })
    // Reconnection backoff
    .backoff(BackoffConfig {
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_secs(30),
        multiplier: 2.0,
        jitter: true,
    })
    // Health monitoring
    .health(HealthConfig {
        ping_interval: Duration::from_secs(20),
        pong_timeout: Duration::from_secs(10),
        data_timeout: Duration::from_secs(30),
        failure_threshold: 3,
    })
    // Sharding
    .max_subscriptions_per_shard(500)
    .shard_selection_strategy(ShardSelectionStrategy::LeastLoaded)
    .hot_switchover(true)
    .auto_rebalance(true)
    .build();
```

## WebSocketHandler Trait

Implement this trait to customize WebSocket behavior:

```rust
pub trait WebSocketHandler: Send + Sync + 'static {
    type Subscription: Clone + Send + Sync + Eq + Hash + Debug + 'static;

    // Required
    fn url(&self, state: &ConnectionState) -> impl Future<Output = String> + Send;
    fn on_connect(&self, state: &ConnectionState) -> impl Future<Output = Vec<Message>> + Send;
    fn on_message(&self, message: Message, state: &ConnectionState) -> impl Future<Output = ()> + Send;
    fn subscriptions(&self) -> Vec<Self::Subscription>;
    fn subscription_message(&self, subscriptions: &[Self::Subscription]) -> Option<Message>;

    // Optional with defaults
    fn on_disconnect(&self, state: &ConnectionState) -> impl Future<Output = ()> + Send { async {} }
    fn on_error(&self, error: &Error, state: &ConnectionState) -> impl Future<Output = bool> + Send { async { true } }
    fn unsubscription_message(&self, subscriptions: &[Self::Subscription]) -> Option<Message> { None }
    fn max_subscriptions_per_shard(&self) -> usize { 500 }
    fn is_heartbeat(&self, message: &Message) -> bool { false }
}
```

## Metrics

Access real-time metrics for monitoring:

```rust
let metrics = manager.metrics();

// Individual values
println!("Connections: {}", metrics.connections());
println!("Messages received: {}", metrics.messages_received());
println!("Errors: {}", metrics.errors());

// Full snapshot for export
let snapshot = metrics.snapshot();
println!("Active connections: {}", snapshot.active_connections);
println!("Total subscriptions: {}", snapshot.total_subscriptions);

// Per-shard metrics
for shard in &snapshot.shards {
    println!(
        "Shard {}: {} subscriptions, connected: {}",
        shard.shard_id,
        shard.subscription_count,
        shard.is_connected
    );
}
```

## Shard Selection Strategies

- `LeastLoaded` (default) - New subscriptions go to the shard with fewest subscriptions
- `RoundRobin` - Distribute subscriptions in round-robin order
- `Sequential` - Fill shards sequentially (first available with capacity)

## Hot Switchover

When enabled, the manager creates a new connection and waits for it to be ready before closing the old one. This ensures no data loss during reconnection.

```rust
// Trigger manual hot switchover for a shard
manager.hot_switchover(0).await?;
```

## License

MIT
