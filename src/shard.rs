use crate::connection::ConnectionCommand;
use std::collections::HashSet;
use std::hash::Hash;
use tokio::sync::mpsc;

/// Represents a single shard's state
#[derive(Debug)]
pub struct Shard<S: Clone + Eq + Hash> {
    /// Shard identifier
    pub id: usize,
    /// Current subscriptions
    pub subscriptions: HashSet<S>,
    /// Command sender for this shard's connection
    pub command_tx: mpsc::Sender<ConnectionCommand>,
    /// Maximum subscriptions for this shard
    pub max_subscriptions: usize,
}

impl<S: Clone + Eq + Hash> Shard<S> {
    /// Create a new shard
    pub fn new(
        id: usize,
        command_tx: mpsc::Sender<ConnectionCommand>,
        max_subscriptions: usize,
    ) -> Self {
        Self {
            id,
            subscriptions: HashSet::new(),
            command_tx,
            max_subscriptions,
        }
    }

    /// Check if this shard has capacity for more subscriptions
    pub fn has_capacity(&self) -> bool {
        self.subscriptions.len() < self.max_subscriptions
    }

    /// Get current subscription count
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Add a subscription
    pub fn add_subscription(&mut self, sub: S) -> bool {
        if self.has_capacity() {
            self.subscriptions.insert(sub)
        } else {
            false
        }
    }

    /// Remove a subscription
    pub fn remove_subscription(&mut self, sub: &S) -> bool {
        self.subscriptions.remove(sub)
    }
}

/// Strategy for selecting which shard to use for new subscriptions
#[derive(Debug, Clone, Copy, Default)]
pub enum ShardSelectionStrategy {
    /// Use the shard with the least subscriptions
    #[default]
    LeastLoaded,
    /// Round-robin selection
    RoundRobin,
    /// Fill shards sequentially (first available)
    Sequential,
}

/// Selects the best shard for a new subscription, excluding specified shards
///
/// This is useful during hot switchover to prevent new subscriptions from going
/// to shards that are in the middle of a connection swap.
pub fn select_shard_excluding<S: Clone + Eq + Hash>(
    shards: &[Shard<S>],
    excluded: &HashSet<usize>,
    strategy: ShardSelectionStrategy,
    last_used: &mut usize,
) -> Option<usize> {
    match strategy {
        ShardSelectionStrategy::LeastLoaded => {
            shards
                .iter()
                .filter(|s| s.has_capacity() && !excluded.contains(&s.id))
                .min_by_key(|s| s.subscription_count())
                .map(|s| s.id)
        }
        ShardSelectionStrategy::RoundRobin => {
            let start = *last_used;
            for i in 0..shards.len() {
                let idx = (start + i + 1) % shards.len();
                if shards[idx].has_capacity() && !excluded.contains(&idx) {
                    *last_used = idx;
                    return Some(idx);
                }
            }
            None
        }
        ShardSelectionStrategy::Sequential => {
            shards
                .iter()
                .find(|s| s.has_capacity() && !excluded.contains(&s.id))
                .map(|s| s.id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_shards(count: usize, max_subs: usize) -> Vec<Shard<String>> {
        (0..count)
            .map(|id| {
                let (tx, _rx) = mpsc::channel(1);
                Shard::new(id, tx, max_subs)
            })
            .collect()
    }

    #[test]
    fn test_shard_capacity() {
        let (tx, _rx) = mpsc::channel(1);
        let mut shard: Shard<String> = Shard::new(0, tx, 100);

        assert!(shard.has_capacity());
        assert_eq!(shard.subscription_count(), 0);

        for i in 0..100 {
            assert!(shard.add_subscription(format!("sub-{}", i)));
        }

        assert!(!shard.has_capacity());
        assert_eq!(shard.subscription_count(), 100);
        assert!(!shard.add_subscription("overflow".to_string()));
    }

    #[test]
    fn test_least_loaded_selection() {
        let mut shards = create_test_shards(3, 100);

        // Add different amounts to each shard
        for i in 0..30 {
            shards[0].add_subscription(format!("s0-{}", i));
        }
        for i in 0..10 {
            shards[1].add_subscription(format!("s1-{}", i));
        }
        for i in 0..50 {
            shards[2].add_subscription(format!("s2-{}", i));
        }

        let mut last = 0;
        let excluded = HashSet::new();
        let selected = select_shard_excluding(&shards, &excluded, ShardSelectionStrategy::LeastLoaded, &mut last);
        assert_eq!(selected, Some(1)); // Shard 1 has least (10)
    }

    #[test]
    fn test_round_robin_selection() {
        let shards = create_test_shards(3, 100);
        let excluded = HashSet::new();

        let mut last = 0;
        assert_eq!(
            select_shard_excluding(&shards, &excluded, ShardSelectionStrategy::RoundRobin, &mut last),
            Some(1)
        );
        assert_eq!(
            select_shard_excluding(&shards, &excluded, ShardSelectionStrategy::RoundRobin, &mut last),
            Some(2)
        );
        assert_eq!(
            select_shard_excluding(&shards, &excluded, ShardSelectionStrategy::RoundRobin, &mut last),
            Some(0)
        );
    }

    #[test]
    fn test_sequential_selection() {
        let mut shards = create_test_shards(3, 2);
        let excluded = HashSet::new();

        let mut last = 0;

        // Fill first shard
        shards[0].add_subscription("a".to_string());
        shards[0].add_subscription("b".to_string());

        // Should now select second shard
        assert_eq!(
            select_shard_excluding(&shards, &excluded, ShardSelectionStrategy::Sequential, &mut last),
            Some(1)
        );
    }

    #[test]
    fn test_no_capacity_returns_none() {
        let mut shards = create_test_shards(2, 1);
        let excluded = HashSet::new();

        shards[0].add_subscription("a".to_string());
        shards[1].add_subscription("b".to_string());

        let mut last = 0;
        assert_eq!(
            select_shard_excluding(&shards, &excluded, ShardSelectionStrategy::LeastLoaded, &mut last),
            None
        );
    }

    #[test]
    fn test_excluded_shards_skipped() {
        let shards = create_test_shards(3, 100);

        // Exclude shard 1 (the one that would be selected by least-loaded)
        let mut excluded = HashSet::new();
        excluded.insert(1);

        let mut last = 0;
        let selected = select_shard_excluding(&shards, &excluded, ShardSelectionStrategy::LeastLoaded, &mut last);
        // Should select shard 0 or 2, not 1
        assert!(selected == Some(0) || selected == Some(2));
        assert_ne!(selected, Some(1));
    }
}
