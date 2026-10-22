# CI coordination ownership

This package owns CI failure assessments, PR-scoped holds, batch files, and coordinator lifecycle. It uses the configured TaskHarness for history, dispatch and archival; do not construct concrete providers, build native CLI arguments, or add an MCP server here. Provider adapters own unloaded-task startup and delivery to an existing owner.

Keep polling deterministic. Only new current-head failures (or changed task history for a deferred assessment) justify another coordinator. A successful complete repository listing is required before deleting closed-PR state. Commit changes clear failure decisions, never explicit task holds. Save decisions before archiving coordinators or deleting batch files.
