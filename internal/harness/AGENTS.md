# Harness ownership

The factory owns configured provider selection. Application entry points and PR orchestration depend on Harness, never concrete providers or native CLI flags. Discovery owns task matching and fork creation; dispatch starts or resumes the cached fork.

Each provider owns native arguments and identifier validation. Use the managed process runner so cancellation reaches wrapper descendants and cleanup finishes before returning.
