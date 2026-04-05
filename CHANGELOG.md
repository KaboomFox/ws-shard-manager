# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] - 2026-04-05

### Fixed

- Silent data stalls when a protocol mixes real data messages with non-data
  text traffic (subscription acks, status frames, empty-text keepalives). The
  connection's `data_timeout` would never fire because any text message reset
  the timer, even when the handler silently dropped it. Observed in production
  when a Chainlink RTDS subscription was lost during a reconnect storm: the
  shard kept receiving non-data text on the wire, so the data timeout never
  triggered, and one asset's prices went missing for 16+ hours without any
  reconnect attempt.

### Added

- New `WebSocketHandler::is_data_message(&Message) -> bool` trait method with
  a default implementation returning `true` (backward-compatible — all
  existing handlers behave identically). Override this in handlers whose
  protocols send non-data text traffic the transport layer can't distinguish
  from real updates. When `is_data_message` returns `false`, the message is
  still passed to `on_message` as before, but it does not reset the data
  timeout, so stalled subscriptions will now correctly trigger reconnection.

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
