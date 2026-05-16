# Structured Backlog

| ID | Item | Priority | Target phase | Rationale | Related module(s) link/search key |
|---|---|---|---|---|---|
| BL-001 | Polish the package-native CLI/TUI operator experience | P2 | Phase 3 — CLI and TUI productization | Core TUI prompts, summaries, timeout, and cancellation messaging are implemented; remaining work is optional advanced diagnostics drill-down. | `src/oznak/cli.py`, `src/oznak/tui.py`, search: `tests/test_package_tui.py` |
| BL-002 | Extend query preprocessing beyond the implemented `QueryRequest` baseline | P2 | Phase 1 — Query subsystem hardening | Basic inbound normalization and opt-in live dialect coverage are implemented; remaining work is advanced filter groups. | `src/oznak/request.py`, `src/oznak/query_builder.py`, search: `QueryRequest` |
| BL-003 | Remove deprecated legacy `src.*` compatibility modules | P1 | Phase 6 — Legacy removal | Legacy modules now emit deprecation warnings; remove them only after downstream consumers have moved to `oznak.*`. | `src/db`, `src/query`, `src/services`, `src/storage`, search: `DeprecationWarning` |
