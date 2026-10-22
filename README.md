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

Bifrost checks completed failures on each open PR's current head commit, including GitHub check runs and legacy commit statuses. Cancelled and pending checks do not trigger delivery. New failures and changed review threads for a PR are combined into one prompt and sent through the same cached fork. No coordinator or Desktop chat tools are needed.

The fork verifies the live PR, head and check results, investigates failures, and uses `post-push-validation` and `task-implementation` within its approved scope. Once the fork acknowledges startup, Bifrost records the reported CI attempts for that PR head and does not send those attempts again. New commits or newly failed reruns are eligible. Dispatch errors are logged and retried on a later poll. Bifrost does not track whether an acknowledged fork later finishes or crashes. The fork must check whether failures have recovered before doing work.

CI information is stored alongside routes and review fingerprints in `state.json`, under `ci["owner/repo#42"]`:

```json
{
  "head_sha": "current-commit",
  "reported": {"check:123:completion-timestamp": true},
  "paused": true,
  "reason": "CI requires credentials from a human",
  "resume_condition": "Credentials have been restored"
}
```

If further CI messages will not help, the fork can set `paused`, `reason`, and `resume_condition` in its existing PR entry. The prompt supplies the state path and lock instructions. Bifrost rereads the state before dispatch, preserves pause changes when saving delivery results, and logs skipped CI follow-ups. Pauses survive new commits and process restarts. Review comments continue through the normal flow.

Both the fork and Bifrost must acquire an exclusive OS file lock on `<state_file>.update.lock` before reading and updating state, read the latest contents after acquiring it, and atomically replace the state file before releasing it. On macOS/Linux, Python `fcntl.flock(lock, fcntl.LOCK_EX)` interoperates with Bifrost's lock. Never delete the lock file or recreate a removed PR entry. This short transaction lock is separate from the `.lock` held for the service's lifetime.

To resume, use the same locked update to set `paused` to `false` and clear `reason` and `resume_condition`. No restart or GitHub comment is needed. Unreported current failures become eligible after resuming; already reported attempts remain suppressed. A successful complete repository scan removes all state for PRs that are closed or merged. Failed scans never infer closure.

The CLI exposes only `-config` and `-once` (plus standard help). The main configuration is read at startup; per-PR pause state is read during polling and immediately before CI delivery.

## Attribution

The GitHub review-thread query and pagination are adapted from [prdash](https://github.com/danielwolfman/prdash). See `THIRD_PARTY_NOTICES.md`.
