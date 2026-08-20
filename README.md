# Bifrost

Bifrost is a small local bridge from GitHub pull-request feedback to an AI coding harness. It polls configured repositories every five minutes, detects new or updated unresolved inline review threads, and sends one critical-review prompt per affected PR to the mapped agent session.

The first harness is Codex CLI. The Go interface in `internal/harness` is the extension point for other local harnesses.

## Current behavior

- Lists open PRs in explicitly configured repositories, optionally filtered by PR author.
- Reads complete unresolved inline review threads through GitHub GraphQL pagination.
- Emits existing unresolved threads on the first run, then only new or changed thread versions.
- Batches all changed threads for one PR into one message.
- Resumes the mapped Codex session when one exists.
- Starts a new Codex session and records its ID when the PR has no mapping.
- Replaces a stale mapping only when Codex reports that the mapped task no longer exists.
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

`GH_TOKEN` or `GITHUB_TOKEN` takes precedence over `gh auth token`. Relative state, mapping-directory, and working-directory paths are resolved from the config directory; `~/...` is also supported. Set `dispatch_timeout` to another positive [Go duration](https://pkg.go.dev/time#ParseDuration) when a task legitimately needs longer than the default `30m`.

An empty `authors` list monitors every open PR in that repository. Restrict repositories and authors carefully: review comments are untrusted input delivered to an agent with access to the configured checkout. Version 1 does not enforce a reviewer allowlist. Bifrost does not enable `--approve-for-me` by default; add harness arguments only when the repository and reviewers are trusted.

The polling-only `GH_TOKEN` and `GITHUB_TOKEN` environment variables are removed from the Codex child process. This is not a credential sandbox: Codex still runs as the current OS user and may be able to use credentials stored by tools such as `gh`. Use a dedicated, least-privileged local environment for stronger isolation.

Codex stderr is used only for bounded internal error classification. Bifrost does not copy arbitrary child stderr into its normal logs.

## PR-to-session mappings

Each PR has an independent file under the configured mapping directory. For `owner/repository#42`, a Codex task that opens the PR writes `mappings/owner/repository/42.json`:

```json
{
  "version": 1,
  "harness": "codex",
  "session_id": "019c0000-0000-7000-8000-000000000000"
}
```

Owner and repository path segments are lowercase. If the PR file is absent, Bifrost starts `codex exec` in the repository's configured working directory and writes the returned session ID. If the file exists, it uses `codex exec resume` for that session.

Earlier pre-release configurations used one `mapping_file` (normally `mappings.json`). Bifrost now stops with a migration message when it detects that format; move each entry to the per-PR layout above, then replace `mapping_file` with `mapping_directory`.

On macOS and Linux, Bifrost writes its files atomically with user-only permissions. External writers should also atomically replace only the affected PR file. Independent files prevent concurrent updates for different PRs from overwriting one another.

## Scope

Bifrost currently targets macOS and Linux and has no UI, webhook listener, database, dynamic plugin loading, reviewer allowlist, per-PR worktree provisioning, or automatic GitHub-thread resolution. GitHub polling remains sequential; queued agent dispatches use at most two workers and the next poll waits for the queue to drain. At most 100 PR dispatches are retained per poll; excess work remains uncommitted and a persisted cursor rotates capacity fairly across later polls. Prompts are capped at 256 KiB and direct the agent to inspect omitted text on the live PR. One-shot mode exits unsuccessfully while PRs or threads remain deferred. Each GitHub response is capped at 8 MiB, with at most 32 MiB of retained review-thread text per PR. Distinct mapped tasks can run concurrently, so give them isolated worktrees; Bifrost-spawned tasks otherwise share the configured checkout.

## Attribution

The GitHub review-thread query and pagination are adapted from [prdash](https://github.com/danielwolfman/prdash). See `THIRD_PARTY_NOTICES.md`.
