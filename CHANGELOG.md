# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-03-29

### Added

- Add automated changelog generation with git-cliff
- Add README version update to release workflow
- Add text-based PING/PONG support for application-level heartbeats

### Changed

- Allow disabling WebSocket ping/pong via Option<Duration>

### Fixed

- Fix test: add default for new HealthConfig fields
## [0.1.1] - 2026-02-07

### Fixed

- Fix dead shard recovery and subscription replay on reconnect
## [0.1.0] - 2026-02-07

### Added

- Initial release: WebSocket shard manager with auto-reconnection
- Add debug logging for message flow and hot switchover
- Add sequence validation and connection headers to WebSocket handler
- Add badges and MSRV section to README
- Add release workflow for automated publishing

### Changed

- Major reliability and security improvements
- Skip data timeout for shards with few subscriptions
- Improve switchover reliability and logging security
- Relax url dependency constraint to allow 2.x versions
- Reduce hot switchover log verbosity

### Documentation

- Document multi-IP and proxy support in README

### Fixed

- Fix CI: use dtolnay/rust-toolchain instead of rust-action
- Fix code review issues: race conditions, API, and docs
- Fix data timeout detection and add handler accessor
- Fix hot switchover connection recovery and metrics race
- Fix CI: formatting, clippy, docs, and MSRV compatibility
- Fix CI and clean up package metadata for release

### Reverted

- Revert: Keep data timeout for all shards

### Security

- Sanitize URLs in logs and clean up package contents
[0.2.1]: https://github.com/KaboomFox/ws-shard-manager/compare/v0.1.1...v0.2.1
[0.1.1]: https://github.com/KaboomFox/ws-shard-manager/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/KaboomFox/ws-shard-manager/releases/tag/v0.1.0

