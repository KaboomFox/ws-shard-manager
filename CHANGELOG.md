# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2024-01-08

### Added

- Initial release
- Auto-reconnection with exponential backoff and full jitter
- WebSocket sharding with configurable subscription limits
- Hot switchover support (connect new before disconnecting old)
- Auto-rebalancing when subscriptions exceed capacity
- Health monitoring via ping/pong and data timeouts
- Comprehensive metrics with snapshot export
- Panic recovery for handler code
- Configurable shard selection strategies (LeastLoaded, RoundRobin, Sequential)
- Configuration validation via `try_build()`
- Smart health check scheduling (replaces fixed polling)
- Application-level heartbeat detection via `is_heartbeat()`
- APIs for querying shard subscriptions
