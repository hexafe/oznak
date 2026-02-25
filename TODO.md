# Structured Backlog

| ID | Item | Priority | Target phase | Rationale | Related module(s) link/search key |
|---|---|---|---|---|---|
| BL-001 | Decide and implement primary UX surface (TUI vs web GUI) | P1 | Phase 3 — API and CLI productization | UX direction is currently undecided; locking the primary interface is required before deeper product polish and test investment. | `cli/`, `api/`, search: `tests/cli`, `api/routes` |
| BL-002 | Build preprocessing pipeline for inbound query/input normalization | P0 | Phase 1 — Query subsystem hardening | Preprocessing is a blocker for reliable validation and predictable query execution across callers. | `query/`, `services/`, search: `filter_parser`, `multi_db_fetcher` |
