use crate::config::HealthConfig;
use std::time::Instant;
use tokio::time::Duration;

/// Tracks health state for a single connection
#[derive(Debug)]
pub struct HealthMonitor {
    config: HealthConfig,

    /// Time of last ping sent
    last_ping_sent: Option<Instant>,

    /// Time of last pong received
    last_pong_received: Option<Instant>,

    /// Time of last data received (message or heartbeat)
    last_data_received: Option<Instant>,

    /// Number of consecutive ping failures (no pong received)
    consecutive_ping_failures: u32,

    /// Whether we're currently waiting for a pong
    waiting_for_pong: bool,
}

impl HealthMonitor {
    /// Create a new health monitor
    pub fn new(config: HealthConfig) -> Self {
        Self {
            config,
            last_ping_sent: None,
            last_pong_received: None,
            last_data_received: Some(Instant::now()), // Start with "just received data"
            consecutive_ping_failures: 0,
            waiting_for_pong: false,
        }
    }

    /// Record that we sent a ping
    pub fn record_ping_sent(&mut self) {
        self.last_ping_sent = Some(Instant::now());
        self.waiting_for_pong = true;
    }

    /// Record that we received a pong
    pub fn record_pong_received(&mut self) {
        self.last_pong_received = Some(Instant::now());
        self.consecutive_ping_failures = 0;
        self.waiting_for_pong = false;
    }

    /// Record that we received data (message or heartbeat)
    pub fn record_data_received(&mut self) {
        self.last_data_received = Some(Instant::now());
    }

    /// Check if we should send a ping now
    pub fn should_send_ping(&self) -> bool {
        if self.waiting_for_pong {
            return false; // Don't send another ping while waiting
        }

        match self.last_ping_sent {
            None => true, // Never sent a ping
            Some(last) => last.elapsed() >= self.config.ping_interval,
        }
    }

    /// Check if pong is overdue and record failure if so.
    ///
    /// This method has side effects: it increments the failure counter
    /// and resets the waiting_for_pong flag when a timeout is detected.
    /// Only call this once per timeout check cycle.
    pub fn check_and_record_pong_timeout(&mut self) -> bool {
        if !self.waiting_for_pong {
            return false;
        }

        match self.last_ping_sent {
            None => false,
            Some(last) => {
                if last.elapsed() >= self.config.pong_timeout {
                    self.consecutive_ping_failures += 1;
                    self.waiting_for_pong = false; // Reset to allow next ping
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Check if data timeout has been exceeded
    pub fn is_data_timeout(&self) -> bool {
        match self.last_data_received {
            None => true,
            Some(last) => last.elapsed() >= self.config.data_timeout,
        }
    }

    /// Reset data timeout timer (used when hot switchover is requested)
    pub fn reset_data_timeout(&mut self) {
        self.last_data_received = Some(Instant::now());
    }

    /// Check if the connection is unhealthy based on consecutive failures
    pub fn is_unhealthy(&self) -> bool {
        self.consecutive_ping_failures >= self.config.failure_threshold
    }

    /// Get time until next ping should be sent
    pub fn time_until_next_ping(&self) -> Duration {
        if self.waiting_for_pong {
            // If waiting for pong, next action is checking for timeout
            match self.last_ping_sent {
                None => Duration::ZERO,
                Some(last) => self
                    .config
                    .pong_timeout
                    .saturating_sub(last.elapsed()),
            }
        } else {
            match self.last_ping_sent {
                None => Duration::ZERO,
                Some(last) => self
                    .config
                    .ping_interval
                    .saturating_sub(last.elapsed()),
            }
        }
    }

    /// Get time until data timeout
    pub fn time_until_data_timeout(&self) -> Duration {
        match self.last_data_received {
            None => Duration::ZERO,
            Some(last) => self
                .config
                .data_timeout
                .saturating_sub(last.elapsed()),
        }
    }

    /// Get the consecutive ping failure count
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_ping_failures
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> HealthConfig {
        HealthConfig {
            ping_interval: Duration::from_millis(100),
            pong_timeout: Duration::from_millis(50),
            data_timeout: Duration::from_millis(200),
            failure_threshold: 3,
        }
    }

    #[test]
    fn test_initial_state() {
        let mut monitor = HealthMonitor::new(test_config());
        assert!(monitor.should_send_ping()); // Never sent a ping
        assert!(!monitor.check_and_record_pong_timeout());
        assert!(!monitor.is_data_timeout());
        assert!(!monitor.is_unhealthy());
    }

    #[test]
    fn test_ping_pong_cycle() {
        let mut monitor = HealthMonitor::new(test_config());

        // Send ping
        monitor.record_ping_sent();
        assert!(!monitor.should_send_ping()); // Waiting for pong

        // Receive pong
        monitor.record_pong_received();
        assert_eq!(monitor.consecutive_failures(), 0);
    }

    #[tokio::test]
    async fn test_pong_timeout() {
        let mut monitor = HealthMonitor::new(test_config());

        monitor.record_ping_sent();

        // Wait for pong timeout
        tokio::time::sleep(Duration::from_millis(60)).await;

        assert!(monitor.check_and_record_pong_timeout());
        assert_eq!(monitor.consecutive_failures(), 1);
    }

    #[tokio::test]
    async fn test_unhealthy_after_threshold() {
        let mut monitor = HealthMonitor::new(test_config());

        for _ in 0..3 {
            monitor.record_ping_sent();
            tokio::time::sleep(Duration::from_millis(60)).await;
            monitor.check_and_record_pong_timeout(); // Process timeout
        }

        assert!(monitor.is_unhealthy());
    }

    #[test]
    fn test_data_received_resets_timeout() {
        let mut monitor = HealthMonitor::new(test_config());
        monitor.record_data_received();
        assert!(!monitor.is_data_timeout());
    }
}
