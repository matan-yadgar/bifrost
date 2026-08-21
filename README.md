# Bifrost

Bifrost is a small local bridge from GitHub pull-request feedback to an AI coding harness. It polls configured repositories every five minutes, detects new or updated unresolved inline review threads, and sends one critical-review prompt per affected PR to the task that created it.

The first harness is Codex CLI. The Go interface in `internal/harness` is the extension point for other local harnesses.

## Current behavior

- Lists open PRs in explicitly configured repositories, optionally filtered by PR author.
- Reads complete unresolved inline review threads through GitHub GraphQL pagination.
- Emits existing unresolved threads on the first run, then only new or changed thread versions.
- Batches all changed threads for one PR into one message.
- Discovers the creating Codex task by searching local task history for the exact PR URL and head branch, then verifies that both occur together in a final assistant response.
- Resumes the uniquely matching Codex task, or starts a new one when no task matches.
- Keeps its private PR-to-task route cache in `state.json`; no second actor or mapping configuration is required.
- Allows only one Bifrost process per state file. A second process exits immediately; the operating system releases the lock if the owner exits or crashes.
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

Each resolved state file has a neighboring `.lock` file. Processes configured with different state files can run at the same time; processes configured with the same state file cannot.

Configurations from earlier Bifrost versions may keep `mapping_directory` or `mapping_file`. Valid records are atomically imported into `state.json`; an existing route in `state.json` wins. The legacy files are left untouched and can be removed with the deprecated config fields after a successful startup.

An empty `authors` list monitors every open PR in that repository. Restrict repositories and authors carefully: review comments are untrusted input delivered to an agent with access to the configured checkout. Version 1 does not enforce a reviewer allowlist. Bifrost does not enable `--approve-for-me` by default; add harness arguments only when the repository and reviewers are trusted.

The polling-only `GH_TOKEN` and `GITHUB_TOKEN` environment variables are removed from the Codex child process. This is not a credential sandbox: Codex still runs as the current OS user and may be able to use credentials stored by tools such as `gh`. Use a dedicated, least-privileged local environment for stronger isolation.

Codex stderr is used only for bounded internal error classification. Bifrost does not copy arbitrary child stderr into its normal logs.

## Codex task discovery

Bifrost asks the local Codex app server to search active and archived local task history for the exact PR URL, then checks those candidates for the exact head-branch name. Paginated turn and item history must show both boundary-delimited strings together in one final assistant response. Legacy history without message phases uses only the terminal assistant message of a completed turn. This excludes user prompts and intermediate commentary from the evidence used to route feedback.

Use this convention when a Codex task opens a PR: include the exact PR URL and exact head branch in that task's final response. No task-name convention or mapping-file write is needed.

If exactly one task matches, Bifrost resumes it. If none match, Bifrost starts a new task in the configured working directory. If multiple tasks match or discovery is unavailable, Bifrost reports the error and leaves the review feedback pending rather than guessing or creating a duplicate task.

This convention is a practical local discovery heuristic, not an authenticated GitHub-to-Codex identity link. A different task whose final response deliberately includes both exact values can still create ambiguity or a false match. Use Bifrost only with trusted local Codex task history; a future harness with authoritative task metadata should validate that metadata in its adapter.

Successful discovery and newly started task IDs are cached under `routes` in `state.json`. This is private Bifrost state, not an integration contract for PR-creating tasks. The cache avoids repeated discovery and is pruned with delivery fingerprints when a successfully listed repository no longer reports the PR as open.

The harness interface separates discovery from dispatch. Codex currently implements discovery through the experimental local `codex app-server --stdio` API and dispatch through `codex exec`; a future Claude or Grok adapter can implement the same Go interface without changing GitHub polling or queue behavior. A Codex CLI update may require a small app-server adapter update while that API remains experimental.

## Scope

Bifrost currently targets macOS and Linux and has no UI, webhook listener, database, dynamic plugin loading, reviewer allowlist, per-PR worktree provisioning, or automatic GitHub-thread resolution. GitHub polling remains sequential; queued agent dispatches use at most two workers and the next poll waits for the queue to drain. At most 100 PR dispatches are retained per poll; excess work remains uncommitted and a persisted cursor rotates capacity fairly across later polls. Prompts are capped at 256 KiB and direct the agent to inspect omitted text on the live PR. One-shot mode exits unsuccessfully while PRs or threads remain deferred. Each GitHub response is capped at 8 MiB, with at most 32 MiB of retained review-thread text per PR. Distinct routed tasks can run concurrently, so give them isolated worktrees; Bifrost-spawned tasks otherwise share the configured checkout.

## Attribution

The GitHub review-thread query and pagination are adapted from [prdash](https://github.com/danielwolfman/prdash). See `THIRD_PARTY_NOTICES.md`.
