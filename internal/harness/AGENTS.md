# Harness ownership

The factory owns configured provider selection. Application entry points and CI orchestration depend on harness interfaces, never concrete providers or native CLI flags. Keep review dispatch on Harness and task lifecycle operations on TaskHarness.

Each provider owns native arguments, identifier validation, recent-history conversion (including an opaque revision), and structured-result options. Archive through the managed command runner so cancellation reaches wrapper descendants and cleanup finishes before returning.
