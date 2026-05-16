# Structured Backlog

| ID | Item | Priority | Target phase | Rationale | Related module(s) link/search key |
|---|---|---|---|---|---|
| BL-001 | Polish the package-native CLI/TUI operator experience | P1 | Phase 3 — CLI and TUI productization | CLI and TUI are now the primary standalone surfaces; remaining work is clearer diagnostics and cancellation ergonomics. | `src/oznak/cli.py`, `src/oznak/tui.py`, search: `tests/test_package_tui.py` |
| BL-002 | Extend query preprocessing beyond the implemented `QueryRequest` baseline | P2 | Phase 1 — Query subsystem hardening | Basic inbound normalization is implemented; remaining work is live-dialect validation and advanced filter groups. | `src/oznak/request.py`, `src/oznak/query_builder.py`, search: `QueryRequest` |
