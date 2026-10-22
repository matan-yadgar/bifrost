# CI coordination ownership

This package owns CI failure assessments, PR-scoped holds, batch files, and coordinator lifecycle. It uses native Codex task-history reads and the existing Codex launcher; do not add an MCP server. Wake an unloaded task with native `exec resume`, falling back to `codex queue` only for the explicit active-writer conflict. Queuing alone cannot start an unloaded task.

Keep polling deterministic. Only new current-head failures (or changed task history for a deferred assessment) justify another coordinator. A successful complete repository listing is required before deleting closed-PR state. Commit changes clear failure decisions, never explicit task holds. Save decisions before archiving coordinators or deleting batch files.
