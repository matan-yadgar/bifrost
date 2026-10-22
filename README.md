# Bifrost

Bifrost is a small local bridge from GitHub pull-request feedback to an AI coding harness. It polls configured repositories every five minutes, detects new or updated unresolved inline review threads, and sends one critical-review prompt per affected PR to the task that created it.

The first harness is Codex CLI. The Go interface in `internal/harness` is the extension point for other local harnesses.

## Current behavior

- Lists open PRs in explicitly configured repositories, optionally filtered by PR author.
- Reads complete unresolved inline review threads through GitHub GraphQL pagination.
- Emits existing unresolved threads on the first run, then only new or changed thread versions.
- Batches all changed threads for one PR into one message.
- Discovers the creating Codex task from a labeled task name in the PR description.
- Forks the uniquely matching Codex task and resumes that child, or starts a new task when no task matches.
- Keeps its private PR-to-task route cache in `state.json`; no second actor or mapping configuration is required.
- Allows only one Bifrost process per state file. A second process exits immediately; the operating system releases the lock if the owner exits or crashes.
- Writes logs to the configured directory while continuing to show them in the terminal. Every start creates a new file, and files rotate after 1,000 lines.
- Clears stale routes for batched re-discovery on the next polling cycle, and removes route and delivery state after a successful scan shows that a PR is no longer open.
- Marks a thread version delivered only after Codex exits successfully. Resolved threads are forgotten so reopening one emits it again.
- Cancels a dispatch after 30 minutes by default and terminates its child process tree; timed-out thread versions remain pending.
- Instructs the agent to validate every comment and document rejected feedback instead of blindly implementing it.
- Uses a bounded two-worker queue built with Go channels—no queue dependency. Different agent sessions can run in parallel, while messages for the same session run serially. New sessions sharing a configured checkout are started serially.

## Install

Requirements: Go, GitHub CLI authenticated with `gh auth login`, and an authenticated Codex CLI.

```sh
go install github.com/matan-yadgar/bifrost/cmd/bifrost@latest
```

Copy `config.example.json` to `~/.config/bifrost/config.json`, update the repository and local working directory, then run:

```sh
bifrost
```

Use a different config or perform one polling pass:

```sh
bifrost -config /path/to/config.json -once
```

`GH_TOKEN` or `GITHUB_TOKEN` takes precedence over `gh auth token`. Relative state and working-directory paths are resolved from the config directory; `~/...` is also supported. Set `dispatch_timeout` to another positive [Go duration](https://pkg.go.dev/time#ParseDuration) when a task legitimately needs longer than the default `30m`.

Set `log_directory` to the directory for persistent logs. Relative paths are resolved from the config directory, and existing configs default to `logs` beside the config file. Each process start creates `bifrost_<YYYY-MM-DD_HH-MM-SS>.log`; same-second collisions add `_1`, `_2`, and so on. A new file is created before writing line 1,001. Log retention and deletion are left to the user.

Each resolved state file has a neighboring `.lock` file. Processes configured with different state files can run at the same time; processes configured with the same state file cannot.

An empty `authors` list monitors every open PR in that repository. Restrict repositories and authors carefully: review comments are untrusted input delivered to an agent with access to the configured checkout. Version 1 does not enforce a reviewer allowlist. Bifrost does not enable `--approve-for-me` by default; add harness arguments only when the repository and reviewers are trusted.

The polling-only `GH_TOKEN` and `GITHUB_TOKEN` environment variables are removed from the Codex child process. This is not a credential sandbox: Codex still runs as the current OS user and may be able to use credentials stored by tools such as `gh`. Use a dedicated, least-privileged local environment for stronger isolation.

Codex stderr is used only for bounded internal error classification. Bifrost does not copy arbitrary child stderr into its normal logs.

## Codex task discovery

Use this convention when a Codex task opens a PR: add `Bifrost task: <exact task name>` on its own line anywhere in the PR description. Other description content is ignored.

Bifrost reads that value, lists active and archived tasks through the local Codex app server once per discovery batch, and requires an exact user-visible name match.

If exactly one task matches, Bifrost creates a persistent Codex fork containing its history and sends review feedback to that child. This avoids competing with the Desktop app for ownership of the creator task. Forks are named `Bifrost: owner/repo#number` for visibility in Codex. If none match, Bifrost starts a new task in the configured working directory. If multiple tasks match or discovery is unavailable, Bifrost reports the error and leaves the review feedback pending rather than guessing or creating a duplicate task.

This convention is a practical local discovery heuristic, not an authenticated GitHub-to-Codex identity link. Use Bifrost only with trusted PR descriptions and local task history.

Forked and newly started task IDs are cached under `routes` in `state.json`. Later feedback resumes the cached Bifrost-owned task rather than creating another fork. This route cache is authoritative Bifrost state, not an integration contract for PR-creating tasks. It is pruned with delivery fingerprints when a successfully listed repository no longer reports the PR as open.

The harness interface separates discovery from dispatch. Codex currently implements discovery through the experimental local `codex app-server --stdio` API and dispatch through `codex exec`; a future Claude or Grok adapter can implement the same Go interface without changing GitHub polling or queue behavior. A Codex CLI update may require a small app-server adapter update while that API remains experimental.

## Scope

Bifrost currently targets macOS and Linux and has no UI, webhook listener, database, dynamic plugin loading, reviewer allowlist, per-PR worktree provisioning, or automatic GitHub-thread resolution. GitHub polling remains sequential; queued agent dispatches use at most two workers and the next poll waits for the queue to drain. At most 100 PR dispatches are retained per poll; excess work remains uncommitted and a persisted cursor rotates capacity fairly across later polls. Prompts are capped at 256 KiB and direct the agent to inspect omitted text on the live PR. One-shot mode exits unsuccessfully while PRs or threads remain deferred. Each GitHub response is capped at 8 MiB, with at most 32 MiB of retained review-thread text per PR. Distinct routed tasks can run concurrently, so give them isolated worktrees; Bifrost-spawned tasks otherwise share the configured checkout.

## CI follow-up

Set `"monitor_ci": true` to enable CI monitoring in addition to review-comment monitoring. It uses the existing `poll_interval` and each repository's `authors`; CI monitoring requires a nonempty author list. Existing configurations keep CI monitoring disabled.

Bifrost checks completed failures on each open PR's current head commit, including GitHub check runs and legacy commit statuses. Cancelled and pending checks do not trigger a coordinator. A check run ID and completion timestamp distinguish rerun attempts. Previously assessed failures stay quiet; a new head or failed attempt can trigger another assessment.

A fresh Codex coordinator handles up to 25 affected PRs per batch. It receives PR URLs, head SHAs, failed-check links and the `Bifrost task:` names. It reads recent task history through the native app-server API using `bifrost -config /path/to/config.json -read-task 'exact task name or UUID'`, then uses `bifrost -config /path/to/config.json -wake-task TASK_UUID -message MESSAGE` to wake the original task. This adapter starts an unloaded task using native `codex exec resume`; for an explicit active-writer conflict it uses native `codex queue`, preserving Desktop ownership. Queueing alone cannot start an unloaded task. There is no extra MCP server, global Codex configuration change, or fork of the originating task. Keep Codex Desktop running so it can consume queued messages for its tasks. Coordinators require separately stored `gh auth login` credentials: polling-only `GH_TOKEN` and `GITHUB_TOKEN` are removed from their environment, and Bifrost checks this prerequisite before launch. The installed Codex CLI must support `queue`, `archive`, and the app-server task APIs.

The coordinator must recheck live PR state, head SHA, current check outcomes and task context before queueing a message. It asks the originating task to use `post-push-validation` and `task-implementation` within its existing authorization. It respects explicit user pauses, human blockers and repair-cycle limits. Persisted history can lag a running Desktop task; native queuing serializes delivery instead of taking ownership or interrupting its current turn.

CI memory lives in `<state_file>.ci.json`, protected by the same Bifrost instance lock. Decisions are `wake`, `hold`, `wait`, or `ignore`. Holds remain until explicitly released, including across commits:

```sh
bifrost -config /path/to/config.json -release-task TASK_UUID
```

`wait` decisions are reassessed only after the target's persisted history changes. Holds and routing problems are logged with their reasons. Missing or ambiguous task names are reported for human correction rather than guessed; changing the PR's `Bifrost task:` name causes a fresh assessment.

After a result is saved, Bifrost archives the coordinator and deletes its batch directory. If no result arrives within `dispatch_timeout`, the batch is held for investigation; Bifrost does not silently start another coordinator. Failed coordinators keep their task ID, batch and diagnostics; investigate them, then explicitly allow another attempt:

```sh
bifrost -config /path/to/config.json -retry-ci
```

A successful complete repository scan removes CI state (including PR-owned holds) for closed or merged PRs and removes those PRs from pending batches. A new head removes decisions for the previous commit. Shared batches retain their still-open, current-head entries; the coordinator re-reads the batch before waking a task. Failed scans never infer closure. State and temporary artifacts are cleaned up; archived chats and ordinary process logs follow their existing retention behavior. CI monitoring has its own state schema and does not alter review-comment routing.

## Attribution

The GitHub review-thread query and pagination are adapted from [prdash](https://github.com/danielwolfman/prdash). See `THIRD_PARTY_NOTICES.md`.
