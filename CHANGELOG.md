# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0rc1] - 2026-05-16

### Added
- Added package-native CLI/TUI/API/library surfaces under `oznak.*`.
- Added typed profiles, query requests, filters, fetch results, diagnostics,
  cancellation, timeouts, and bounded `max_workers`.
- Added chunked fetch, bounded parallel chunk streaming, streaming CSV export,
  optional Parquet export, and no-DB synthetic chunk benchmarks.
- Added opt-in Docker-backed live MySQL/MSSQL integration test harness.

### Changed
- Deprecated legacy `src.*` modules in favor of package-native imports.
- Updated TUI fetch flow with compact source summaries and clearer timeout or
  cancellation messages.
- Expanded release gates, documentation, public config hygiene, and package
  build checks.

### Fixed
- Fixed default package fetch and chunked fetch execution against real
  SQLAlchemy engines by passing compiled SQL through `sqlalchemy.text`.

## [0.1.1] - 2025-11-27

### Added
- Added `--date-col` option to specify the date column for `--last` ordering.
- Added feedback messages during database fetching process.

### Fixed
- Typos and missing formatted strings.
