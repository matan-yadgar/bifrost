package ci

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/matan-yadgar/bifrost/internal/config"
	githubapi "github.com/matan-yadgar/bifrost/internal/github"
)

const testTask = "019c0000-0000-7000-8000-000000000009"

type fakeSource struct {
	prs    []githubapi.PullRequest
	checks []githubapi.FailedCheck
	err    error
}

func (source *fakeSource) OpenPullRequests(context.Context, string) ([]githubapi.PullRequest, error) {
	return source.prs, source.err
}
func (source *fakeSource) FailedChecks(context.Context, githubapi.PullRequest) ([]githubapi.FailedCheck, error) {
	return source.checks, nil
}

type fakeRuntime struct {
	launches     int
	archived     []string
	revision     string
	archiveError error
}

func (runtime *fakeRuntime) Launch(context.Context, *batch) (string, error) {
	runtime.launches++
	return testTask, nil
}
func (runtime *fakeRuntime) Archive(_ context.Context, id string) error {
	runtime.archived = append(runtime.archived, id)
	return runtime.archiveError
}
func (runtime *fakeRuntime) ReadTask(context.Context, string) (json.RawMessage, error) {
	return json.RawMessage(`{"revision":"` + runtime.revision + `","thread":{"id":"` + testTask + `","turns":[{"id":"` + runtime.revision + `","status":"inProgress"}]}}`), nil
}

func fixture(t *testing.T) (*Monitor, *fakeSource, *fakeRuntime) {
	t.Helper()
	source := &fakeSource{prs: []githubapi.PullRequest{{Repository: "owner/repo", Number: 1, URL: "https://github.com/owner/repo/pull/1", HeadSHA: "sha-1", Author: "me", Body: "Bifrost task: Feature"}}, checks: []githubapi.FailedCheck{{ID: "check:1", Name: "unit"}}}
	runtime := &fakeRuntime{revision: "turn-1"}
	monitor := &Monitor{source: source, runtime: runtime, repositories: []config.Repository{{Name: "owner/repo", Authors: []string{"me"}}}, statePath: filepath.Join(t.TempDir(), "state.ci.json"), logger: log.New(io.Discard, "", 0), timeout: time.Minute}
	return monitor, source, runtime
}

func poll(t *testing.T, monitor *Monitor) {
	t.Helper()
	if err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func complete(t *testing.T, monitor *Monitor, outcome string) {
	t.Helper()
	memory, err := monitor.load()
	if err != nil {
		t.Fatal(err)
	}
	var decisions []decision
	for _, entry := range memory.Pending.Failures {
		decisions = append(decisions, decision{FailureID: entry.ID, TaskID: testTask, Outcome: outcome, Reason: "test decision", Revision: "turn-1"})
	}
	if err := writeJSON(filepath.Join(memory.Pending.Directory, "result.json"), map[string]any{"decisions": decisions}); err != nil {
		t.Fatal(err)
	}
	poll(t, monitor)
}

func TestFailuresDeduplicateAcrossRestartAndNewAttempt(t *testing.T) {
	t.Parallel()
	monitor, source, runtime := fixture(t)
	poll(t, monitor)
	poll(t, monitor)
	if runtime.launches != 1 {
		t.Fatalf("duplicate pending launches: %d", runtime.launches)
	}
	complete(t, monitor, "wake")
	monitor.statePath = filepath.Clean(monitor.statePath)
	poll(t, monitor)
	if runtime.launches != 1 {
		t.Fatal("same failure was redispatched")
	}
	source.checks = append(source.checks, githubapi.FailedCheck{ID: "check:2", Name: "lint"})
	poll(t, monitor)
	memory, _ := monitor.load()
	if runtime.launches != 2 || len(memory.Pending.Failures[0].Checks) != 1 || memory.Pending.Failures[0].Checks[0].Name != "lint" {
		t.Fatal("did not isolate new failure")
	}
	complete(t, monitor, "wake")
	source.checks = source.checks[:1]
	poll(t, monitor)
	if runtime.launches != 2 {
		t.Fatal("resolving a different check retriggered a known failure")
	}
	source.prs[0].HeadSHA = "sha-2"
	poll(t, monitor)
	if runtime.launches != 3 {
		t.Fatal("new head not assessed")
	}
}

func TestHoldSurvivesNewHeadUntilExplicitRelease(t *testing.T) {
	t.Parallel()
	monitor, source, runtime := fixture(t)
	poll(t, monitor)
	complete(t, monitor, "hold")
	source.prs[0].HeadSHA = "sha-2"
	source.checks[0].ID = "check:2"
	poll(t, monitor)
	if runtime.launches != 1 {
		t.Fatal("held task was reengaged")
	}
	if err := monitor.ReleaseTask(testTask); err != nil {
		t.Fatal(err)
	}
	poll(t, monitor)
	if runtime.launches != 2 {
		t.Fatal("released task not reassessed")
	}
}

func TestWaitingTaskOnlyReassessedAfterHistoryChanges(t *testing.T) {
	t.Parallel()
	monitor, _, runtime := fixture(t)
	poll(t, monitor)
	complete(t, monitor, "wait")
	poll(t, monitor)
	if runtime.launches != 1 {
		t.Fatal("unchanged active task was reassessed")
	}
	runtime.revision = "turn-2"
	poll(t, monitor)
	if runtime.launches != 2 {
		t.Fatal("task history change not reassessed")
	}
}

func TestClosedPRCleanupRequiresSuccessfulScan(t *testing.T) {
	t.Parallel()
	monitor, source, runtime := fixture(t)
	poll(t, monitor)
	memory, _ := monitor.load()
	directory := memory.Pending.Directory
	source.prs = nil
	source.err = errors.New("partial listing failed")
	if err := monitor.RunOnce(context.Background()); err == nil {
		t.Fatal("expected scan error")
	}
	memory, _ = monitor.load()
	if len(memory.PRs) != 1 || memory.Pending == nil {
		t.Fatal("failed scan erased state")
	}
	source.err = nil
	poll(t, monitor)
	memory, _ = monitor.load()
	if len(memory.PRs) != 0 || memory.Pending != nil {
		t.Fatal("closed PR state retained")
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("batch not removed: %v", err)
	}
	if len(runtime.archived) != 1 {
		t.Fatal("coordinator not archived")
	}
}

func TestSharedBatchKeepsOpenPRAndPurgesClosedEntry(t *testing.T) {
	t.Parallel()
	monitor, source, _ := fixture(t)
	second := source.prs[0]
	second.Number = 2
	source.prs = append(source.prs, second)
	poll(t, monitor)
	source.prs = source.prs[1:]
	poll(t, monitor)
	memory, _ := monitor.load()
	if len(memory.PRs) != 1 || len(memory.Pending.Failures) != 1 || memory.Pending.Failures[0].PRKey != "owner/repo#2" {
		t.Fatal("shared batch pruned incorrectly")
	}
	var entries []failure
	data, _ := os.ReadFile(filepath.Join(memory.Pending.Directory, "batch.json"))
	_ = json.Unmarshal(data, &entries)
	if len(entries) != 1 || entries[0].PRKey != "owner/repo#2" {
		t.Fatal("closed PR remained in batch file")
	}
}

func TestArchiveFailureDoesNotLoseSavedDecision(t *testing.T) {
	t.Parallel()
	monitor, _, runtime := fixture(t)
	poll(t, monitor)
	memory, _ := monitor.load()
	report := map[string]any{"decisions": []decision{{FailureID: memory.Pending.Failures[0].ID, TaskID: testTask, Outcome: "hold", Reason: "human approval"}}}
	if err := writeJSON(filepath.Join(memory.Pending.Directory, "result.json"), report); err != nil {
		t.Fatal(err)
	}
	runtime.archiveError = errors.New("busy")
	if err := monitor.RunOnce(context.Background()); err == nil {
		t.Fatal("expected archive error")
	}
	memory, _ = monitor.load()
	if memory.PRs["owner/repo#1"].Hold == nil || !memory.Pending.Applied {
		t.Fatal("decision not saved before cleanup")
	}
	runtime.archiveError = nil
	poll(t, monitor)
	memory, _ = monitor.load()
	if memory.Pending != nil || runtime.launches != 1 {
		t.Fatal("cleanup retry redispatched work")
	}
}

func TestAuthorFilterAndInvalidReport(t *testing.T) {
	t.Parallel()
	monitor, source, runtime := fixture(t)
	source.prs[0].Author = "someone-else"
	poll(t, monitor)
	if runtime.launches != 0 {
		t.Fatal("wrong author's task launched")
	}
	source.prs[0].Author = "me"
	poll(t, monitor)
	memory, _ := monitor.load()
	if err := writeJSON(filepath.Join(memory.Pending.Directory, "result.json"), map[string]any{"decisions": []decision{}}); err != nil {
		t.Fatal(err)
	}
	if err := monitor.RunOnce(context.Background()); err == nil {
		t.Fatal("invalid report accepted")
	}
	pollError := monitor.RunOnce(context.Background())
	if pollError == nil || runtime.launches != 1 {
		t.Fatal("invalid coordinator report relaunched without investigation")
	}
}

func TestWaitObservedBeforeTaskCompletionIsReassessed(t *testing.T) {
	t.Parallel()
	monitor, _, runtime := fixture(t)
	poll(t, monitor)
	runtime.revision = "completed-turn-1"
	complete(t, monitor, "wait")
	poll(t, monitor)
	if runtime.launches != 2 {
		t.Fatal("completion before report ingestion suppressed failed CI")
	}
}

func TestMissingResultWaitsUntilDeadlineThenRequiresInvestigation(t *testing.T) {
	t.Parallel()
	monitor, _, runtime := fixture(t)
	poll(t, monitor)
	poll(t, monitor)
	if runtime.launches != 1 {
		t.Fatal("pending coordinator duplicated")
	}
	memory, err := monitor.load()
	if err != nil {
		t.Fatal(err)
	}
	memory.Pending.Deadline = time.Now().Add(-time.Minute)
	if err := monitor.save(memory); err != nil {
		t.Fatal(err)
	}
	if err := monitor.RunOnce(context.Background()); err == nil {
		t.Fatal("expired coordinator not reported")
	}
	memory, _ = monitor.load()
	if memory.Pending.Error == "" || runtime.launches != 1 {
		t.Fatal("expired coordinator not retained for investigation")
	}
}

func TestPendingBatchInvalidatesCorrectedOrRemovedRouting(t *testing.T) {
	t.Parallel()
	for _, body := range []string{"Bifrost task: Corrected task", "No routing marker"} {
		t.Run(body, func(t *testing.T) {
			monitor, source, runtime := fixture(t)
			poll(t, monitor)
			memory, err := monitor.load()
			if err != nil {
				t.Fatal(err)
			}
			oldBatch := memory.Pending.Directory
			oldEntry := memory.Pending.Failures[0]
			report := map[string]any{"decisions": []decision{{FailureID: oldEntry.ID, TaskID: testTask, Outcome: "hold", Reason: "old task needs approval"}}}
			if err := writeJSON(filepath.Join(oldBatch, "result.json"), report); err != nil {
				t.Fatal(err)
			}
			source.prs[0].Body = body
			poll(t, monitor)
			memory, err = monitor.load()
			if err != nil {
				t.Fatal(err)
			}
			saved := memory.PRs[oldEntry.PRKey]
			if memory.Pending != nil || len(saved.Decisions) != 0 || saved.Hold != nil {
				t.Fatal("obsolete route result was applied")
			}
			if _, err := os.Stat(oldBatch); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("obsolete batch retained: %v", err)
			}
			poll(t, monitor)
			memory, err = monitor.load()
			if err != nil {
				t.Fatal(err)
			}
			if runtime.launches != 2 || memory.Pending.Failures[0].TaskName != source.prs[0].TaskName() {
				t.Fatal("corrected route was suppressed")
			}
		})
	}
}

func TestRouteChangeRemovesOnlyObsoleteEntryFromSharedBatch(t *testing.T) {
	t.Parallel()
	monitor, source, _ := fixture(t)
	second := source.prs[0]
	second.Number = 2
	source.prs = append(source.prs, second)
	poll(t, monitor)
	source.prs[0].Body = "Bifrost task: Replacement"
	poll(t, monitor)
	memory, err := monitor.load()
	if err != nil {
		t.Fatal(err)
	}
	if memory.Pending == nil || len(memory.Pending.Failures) != 1 || memory.Pending.Failures[0].PRKey != "owner/repo#2" {
		t.Fatal("unaffected pending route was removed")
	}
	data, err := os.ReadFile(filepath.Join(memory.Pending.Directory, "batch.json"))
	if err != nil {
		t.Fatal(err)
	}
	var entries []failure
	if err := json.Unmarshal(data, &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].PRKey != "owner/repo#2" {
		t.Fatal("obsolete route remained authorized in batch file")
	}
}
