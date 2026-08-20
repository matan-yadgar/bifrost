package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	githubapi "github.com/matan-yadgar/bifrost/internal/github"
	"github.com/matan-yadgar/bifrost/internal/harness"
)

type fakeSource struct {
	pullRequest          githubapi.PullRequest
	pullRequests         []githubapi.PullRequest
	threads              []githubapi.ReviewThread
	threadsByPullRequest map[int][]githubapi.ReviewThread
	openError            error
}

func (source *fakeSource) OpenPullRequests(context.Context, string, []string) ([]githubapi.PullRequest, error) {
	if source.openError != nil {
		return nil, source.openError
	}
	if source.pullRequests != nil {
		return source.pullRequests, nil
	}
	return []githubapi.PullRequest{source.pullRequest}, nil
}

func (source *fakeSource) ReviewThreads(_ context.Context, pullRequest githubapi.PullRequest) ([]githubapi.ReviewThread, error) {
	if source.threadsByPullRequest != nil {
		return source.threadsByPullRequest[pullRequest.Number], nil
	}
	return source.threads, nil
}

type fakeHarness struct {
	requests     []harness.Request
	startedID    string
	err          error
	beforeReturn func()
	dispatch     func(harness.Request) (harness.Result, error)
}

func (agentHarness *fakeHarness) Name() string {
	return "codex"
}

func (agentHarness *fakeHarness) Dispatch(_ context.Context, request harness.Request) (harness.Result, error) {
	agentHarness.requests = append(agentHarness.requests, request)
	if agentHarness.dispatch != nil {
		return agentHarness.dispatch(request)
	}
	if agentHarness.beforeReturn != nil {
		agentHarness.beforeReturn()
	}
	if request.SessionID != "" {
		return harness.Result{SessionID: request.SessionID}, agentHarness.err
	}
	if agentHarness.startedID == "" {
		agentHarness.startedID = "019c0000-0000-7000-8000-000000000010"
	}
	return harness.Result{SessionID: agentHarness.startedID}, agentHarness.err
}

func TestMonitorDispatchesChangedUnresolvedThreads(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	mappingPath := filepath.Join(directory, "mappings.json")
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	source := &fakeSource{
		pullRequest: githubapi.PullRequest{Repository: "Owner/Repo", Number: 42, Title: "Feature", URL: "https://example/pr/42"},
		threads: []githubapi.ReviewThread{
			{ID: "thread-1", Path: "main.go", Comments: []githubapi.ReviewComment{{
				ID: "comment-1", Author: "reviewer", Body: "consider this", URL: "https://example/comment/1", CreatedAt: now, UpdatedAt: now,
			}}},
			{ID: "thread-2", Path: "other.go", Comments: []githubapi.ReviewComment{{
				ID: "comment-2", Author: "reviewer", Body: "also consider this", URL: "https://example/comment/2", CreatedAt: now, UpdatedAt: now,
			}}},
		},
	}
	agentHarness := &fakeHarness{}
	monitor := New(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, mappingPath)

	result, err := monitor.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Dispatches != 1 || len(agentHarness.requests) != 1 || agentHarness.requests[0].SessionID != "" {
		t.Fatalf("first dispatch = %#v / %#v", result, agentHarness.requests)
	}
	if !strings.Contains(agentHarness.requests[0].Prompt, "Do not blindly implement comments") {
		t.Fatalf("prompt did not contain critical-review policy: %s", agentHarness.requests[0].Prompt)
	}
	if !strings.Contains(agentHarness.requests[0].Prompt, "main.go") || !strings.Contains(agentHarness.requests[0].Prompt, "other.go") {
		t.Fatalf("prompt did not batch both threads: %s", agentHarness.requests[0].Prompt)
	}
	var mappings mappingFile
	readJSON(t, mappingPath, &mappings)
	if mappings.PullRequests["owner/repo#42"].SessionID != agentHarness.startedID {
		t.Fatalf("mappings = %#v", mappings.PullRequests)
	}

	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 0 {
		t.Fatalf("unchanged result/error = %#v / %v", result, err)
	}

	source.threads[0].Comments[0].Body = "updated request"
	source.threads[0].Comments[0].UpdatedAt = now.Add(time.Minute)
	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("updated result/error = %#v / %v", result, err)
	}
	if agentHarness.requests[1].SessionID != agentHarness.startedID {
		t.Fatalf("updated thread did not resume mapped session: %#v", agentHarness.requests[1])
	}

	source.threads[0].IsResolved = true
	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 0 {
		t.Fatalf("resolved result/error = %#v / %v", result, err)
	}
	source.threads[0].IsResolved = false
	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("reopened result/error = %#v / %v", result, err)
	}
}

func TestMonitorPersistsStartedSessionAndRetriesFailedDelivery(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mappingPath := filepath.Join(directory, "mappings.json")
	source := reviewSource()
	dispatchError := context.DeadlineExceeded
	agentHarness := &fakeHarness{
		startedID: "019c0000-0000-7000-8000-000000000011",
		err:       dispatchError,
		beforeReturn: func() {
			external := &mappingFile{Version: mappingSchemaVersion, PullRequests: map[string]mapping{
				"owner/other#7": {Harness: "codex", SessionID: "019c0000-0000-7000-8000-000000000012"},
			}}
			if err := saveJSON(mappingPath, external); err != nil {
				t.Error(err)
			}
		},
	}
	monitor := New(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), mappingPath)

	result, err := monitor.RunOnce(context.Background())
	if err == nil || result.Dispatches != 0 {
		t.Fatalf("failed result/error = %#v / %v", result, err)
	}
	var mappings mappingFile
	readJSON(t, mappingPath, &mappings)
	if mappings.PullRequests["owner/repo#42"].SessionID != agentHarness.startedID || mappings.PullRequests["owner/other#7"].SessionID == "" {
		t.Fatalf("merged mappings = %#v", mappings.PullRequests)
	}

	agentHarness.err = nil
	agentHarness.beforeReturn = nil
	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("retry result/error = %#v / %v", result, err)
	}
	if agentHarness.requests[1].SessionID != agentHarness.startedID {
		t.Fatalf("retry did not resume started session: %#v", agentHarness.requests[1])
	}
}

func TestMonitorTreatsBlankMappedSessionAsUnmapped(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mappingPath := filepath.Join(directory, "mappings.json")
	if err := saveJSON(mappingPath, &mappingFile{Version: mappingSchemaVersion, PullRequests: map[string]mapping{
		"OWNER/REPO#42": {Harness: "codex", SessionID: "  "},
	}}); err != nil {
		t.Fatal(err)
	}
	agentHarness := &fakeHarness{startedID: "019c0000-0000-7000-8000-000000000013"}
	monitor := New(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), mappingPath)

	if _, err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 1 || agentHarness.requests[0].SessionID != "" {
		t.Fatalf("request = %#v", agentHarness.requests)
	}
	var mappings mappingFile
	readJSON(t, mappingPath, &mappings)
	if mappings.PullRequests["owner/repo#42"].SessionID != agentHarness.startedID {
		t.Fatalf("mappings = %#v", mappings.PullRequests)
	}
}

func TestMonitorReplacesMissingMappedSession(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mappingPath := filepath.Join(directory, "mappings.json")
	const oldSessionID = "019c0000-0000-7000-8000-000000000014"
	const newSessionID = "019c0000-0000-7000-8000-000000000015"
	if err := saveJSON(mappingPath, &mappingFile{Version: mappingSchemaVersion, PullRequests: map[string]mapping{
		"owner/repo#42": {Harness: "codex", SessionID: oldSessionID},
	}}); err != nil {
		t.Fatal(err)
	}
	agentHarness := &fakeHarness{dispatch: func(request harness.Request) (harness.Result, error) {
		if request.SessionID == oldSessionID {
			return harness.Result{SessionID: oldSessionID}, harness.ErrSessionNotFound
		}
		if request.SessionID != "" {
			return harness.Result{}, fmt.Errorf("unexpected session ID %q", request.SessionID)
		}
		return harness.Result{SessionID: newSessionID}, nil
	}}
	monitor := New(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), mappingPath)

	result, err := monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	if len(agentHarness.requests) != 2 || agentHarness.requests[0].SessionID != oldSessionID || agentHarness.requests[1].SessionID != "" {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}
	var mappings mappingFile
	readJSON(t, mappingPath, &mappings)
	if mappings.PullRequests["owner/repo#42"].SessionID != newSessionID {
		t.Fatalf("mappings = %#v", mappings.PullRequests)
	}

	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 0 || len(agentHarness.requests) != 2 {
		t.Fatalf("unchanged result/error/requests = %#v / %v / %#v", result, err, agentHarness.requests)
	}
}

func TestMonitorDoesNotCommitDeliveryWithoutSessionID(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	agentHarness := &fakeHarness{dispatch: func(harness.Request) (harness.Result, error) {
		return harness.Result{}, nil
	}}
	monitor := New(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), filepath.Join(directory, "mappings.json"))

	if result, err := monitor.RunOnce(context.Background()); err == nil || result.Dispatches != 0 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	if result, err := monitor.RunOnce(context.Background()); err == nil || result.Dispatches != 0 || len(agentHarness.requests) != 2 {
		t.Fatalf("retry result/error/requests = %#v / %v / %#v", result, err, agentHarness.requests)
	}
}

type blockingHarness struct {
	started          chan string
	release          chan struct{}
	mutex            sync.Mutex
	active           int
	maximum          int
	activeBySession  map[string]int
	maximumBySession map[string]int
}

func (agentHarness *blockingHarness) Name() string {
	return "codex"
}

func (agentHarness *blockingHarness) Dispatch(ctx context.Context, request harness.Request) (harness.Result, error) {
	agentHarness.mutex.Lock()
	agentHarness.active++
	if agentHarness.activeBySession == nil {
		agentHarness.activeBySession = make(map[string]int)
		agentHarness.maximumBySession = make(map[string]int)
	}
	agentHarness.activeBySession[request.SessionID]++
	if agentHarness.active > agentHarness.maximum {
		agentHarness.maximum = agentHarness.active
	}
	if agentHarness.activeBySession[request.SessionID] > agentHarness.maximumBySession[request.SessionID] {
		agentHarness.maximumBySession[request.SessionID] = agentHarness.activeBySession[request.SessionID]
	}
	agentHarness.mutex.Unlock()
	defer func() {
		agentHarness.mutex.Lock()
		agentHarness.active--
		agentHarness.activeBySession[request.SessionID]--
		agentHarness.mutex.Unlock()
	}()
	select {
	case agentHarness.started <- request.SessionID:
	case <-ctx.Done():
		return harness.Result{SessionID: request.SessionID}, ctx.Err()
	}
	select {
	case <-agentHarness.release:
		return harness.Result{SessionID: request.SessionID}, nil
	case <-ctx.Done():
		return harness.Result{SessionID: request.SessionID}, ctx.Err()
	}
}

func TestDispatchQueueRunsDifferentSessionsInParallelAndSerializesOneSession(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mappingPath := filepath.Join(directory, "mappings.json")
	const sharedSessionID = "019c0000-0000-7000-8000-000000000020"
	const otherSessionID = "019c0000-0000-7000-8000-000000000021"
	const thirdSessionID = "019c0000-0000-7000-8000-000000000022"
	if err := saveJSON(mappingPath, &mappingFile{Version: mappingSchemaVersion, PullRequests: map[string]mapping{
		"owner/repo#1": {Harness: "codex", SessionID: sharedSessionID},
		"owner/repo#2": {Harness: "codex", SessionID: sharedSessionID},
		"owner/repo#3": {Harness: "codex", SessionID: otherSessionID},
		"owner/repo#4": {Harness: "codex", SessionID: thirdSessionID},
	}}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	source := &fakeSource{
		pullRequests: []githubapi.PullRequest{
			{Repository: "Owner/Repo", Number: 1, URL: "https://example/pr/1"},
			{Repository: "Owner/Repo", Number: 2, URL: "https://example/pr/2"},
			{Repository: "Owner/Repo", Number: 3, URL: "https://example/pr/3"},
			{Repository: "Owner/Repo", Number: 4, URL: "https://example/pr/4"},
		},
		threadsByPullRequest: map[int][]githubapi.ReviewThread{
			1: {reviewThread("thread-1", now)},
			2: {reviewThread("thread-2", now)},
			3: {reviewThread("thread-3", now)},
			4: {reviewThread("thread-4", now)},
		},
	}
	agentHarness := &blockingHarness{started: make(chan string, 4), release: make(chan struct{}, 4)}
	monitor := New(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), mappingPath)
	type runResult struct {
		result CycleResult
		err    error
	}
	done := make(chan runResult, 1)
	go func() {
		result, err := monitor.RunOnce(context.Background())
		done <- runResult{result: result, err: err}
	}()

	first := receiveStartedSession(t, agentHarness.started)
	second := receiveStartedSession(t, agentHarness.started)
	if first == second {
		t.Fatalf("first sessions = %q / %q", first, second)
	}
	select {
	case sessionID := <-agentHarness.started:
		t.Fatalf("third lane started before worker release: %q", sessionID)
	case run := <-done:
		t.Fatalf("queue completed before dispatches drained: %#v", run)
	case <-time.After(100 * time.Millisecond):
	}
	agentHarness.release <- struct{}{}
	agentHarness.release <- struct{}{}
	third := receiveStartedSession(t, agentHarness.started)
	fourth := receiveStartedSession(t, agentHarness.started)
	agentHarness.release <- struct{}{}
	agentHarness.release <- struct{}{}
	var run runResult
	select {
	case run = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queue to drain")
	}
	if run.err != nil || run.result.Dispatches != 4 {
		t.Fatalf("result/error = %#v / %v", run.result, run.err)
	}
	agentHarness.mutex.Lock()
	defer agentHarness.mutex.Unlock()
	if agentHarness.maximum != 2 {
		t.Fatalf("maximum concurrency = %d", agentHarness.maximum)
	}
	if agentHarness.maximumBySession[sharedSessionID] != 1 {
		t.Fatalf("shared-session concurrency = %d", agentHarness.maximumBySession[sharedSessionID])
	}
	counts := make(map[string]int)
	for _, sessionID := range []string{first, second, third, fourth} {
		counts[sessionID]++
	}
	if counts[sharedSessionID] != 2 || counts[otherSessionID] != 1 || counts[thirdSessionID] != 1 {
		t.Fatalf("observed sessions = %#v", counts)
	}
}

func TestDispatchQueueCancellationLeavesJobsUndelivered(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mappingPath := filepath.Join(directory, "mappings.json")
	mappings := &mappingFile{Version: mappingSchemaVersion, PullRequests: make(map[string]mapping)}
	for number := 1; number <= 5; number++ {
		mappings.PullRequests[fmt.Sprintf("owner/repo#%d", number)] = mapping{Harness: "codex", SessionID: fmt.Sprintf("session-%d", number)}
	}
	if err := saveJSON(mappingPath, mappings); err != nil {
		t.Fatal(err)
	}
	source := sourceWithPullRequests(5)
	agentHarness := &blockingHarness{started: make(chan string, 5), release: make(chan struct{})}
	statePath := filepath.Join(directory, "state.json")
	monitor := New(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, mappingPath)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := monitor.RunOnce(ctx)
		done <- err
	}()
	receiveStartedSession(t, agentHarness.started)
	receiveStartedSession(t, agentHarness.started)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled queue did not stop")
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Threads) != 0 {
		t.Fatalf("canceled jobs were committed: %#v", state.Threads)
	}
}

type selectiveHarness struct {
	mutex    sync.Mutex
	errors   map[string]error
	requests []harness.Request
}

func (agentHarness *selectiveHarness) Name() string {
	return "codex"
}

func (agentHarness *selectiveHarness) Dispatch(_ context.Context, request harness.Request) (harness.Result, error) {
	agentHarness.mutex.Lock()
	defer agentHarness.mutex.Unlock()
	agentHarness.requests = append(agentHarness.requests, request)
	return harness.Result{SessionID: request.SessionID}, agentHarness.errors[request.SessionID]
}

func TestDispatchQueueCommitsOnlySuccessfulJobs(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mappingPath := filepath.Join(directory, "mappings.json")
	if err := saveJSON(mappingPath, &mappingFile{Version: mappingSchemaVersion, PullRequests: map[string]mapping{
		"owner/repo#1": {Harness: "codex", SessionID: "session-success"},
		"owner/repo#2": {Harness: "codex", SessionID: "session-failure"},
	}}); err != nil {
		t.Fatal(err)
	}
	agentHarness := &selectiveHarness{errors: map[string]error{"session-failure": context.DeadlineExceeded}}
	statePath := filepath.Join(directory, "state.json")
	monitor := New(sourceWithPullRequests(2), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, mappingPath)

	result, err := monitor.RunOnce(context.Background())
	if err == nil || result.Dispatches != 1 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.Threads["owner/repo#1"] == nil || state.Threads["owner/repo#2"] != nil {
		t.Fatalf("state = %#v", state.Threads)
	}
	agentHarness.mutex.Lock()
	delete(agentHarness.errors, "session-failure")
	agentHarness.mutex.Unlock()
	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("retry result/error = %#v / %v", result, err)
	}
	agentHarness.mutex.Lock()
	defer agentHarness.mutex.Unlock()
	if len(agentHarness.requests) != 3 || agentHarness.requests[2].SessionID != "session-failure" {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}
}

type staleSessionHarness struct {
	resumeStarted chan struct{}
	resumeRelease chan struct{}
	spawnStarted  chan struct{}
	spawnRelease  chan struct{}
	mutex         sync.Mutex
	activeSpawns  int
	maximumSpawns int
	nextSession   int
}

func (agentHarness *staleSessionHarness) Name() string {
	return "codex"
}

func (agentHarness *staleSessionHarness) Dispatch(_ context.Context, request harness.Request) (harness.Result, error) {
	if request.SessionID != "" {
		agentHarness.resumeStarted <- struct{}{}
		<-agentHarness.resumeRelease
		return harness.Result{SessionID: request.SessionID}, harness.ErrSessionNotFound
	}
	agentHarness.mutex.Lock()
	agentHarness.activeSpawns++
	if agentHarness.activeSpawns > agentHarness.maximumSpawns {
		agentHarness.maximumSpawns = agentHarness.activeSpawns
	}
	agentHarness.nextSession++
	sessionID := fmt.Sprintf("new-session-%d", agentHarness.nextSession)
	agentHarness.mutex.Unlock()
	agentHarness.spawnStarted <- struct{}{}
	<-agentHarness.spawnRelease
	agentHarness.mutex.Lock()
	agentHarness.activeSpawns--
	agentHarness.mutex.Unlock()
	return harness.Result{SessionID: sessionID}, nil
}

func TestStaleSessionsStartSeriallyInOneWorkingDirectory(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mappingPath := filepath.Join(directory, "mappings.json")
	if err := saveJSON(mappingPath, &mappingFile{Version: mappingSchemaVersion, PullRequests: map[string]mapping{
		"owner/repo#1": {Harness: "codex", SessionID: "stale-1"},
		"owner/repo#2": {Harness: "codex", SessionID: "stale-2"},
	}}); err != nil {
		t.Fatal(err)
	}
	agentHarness := &staleSessionHarness{
		resumeStarted: make(chan struct{}, 2), resumeRelease: make(chan struct{}),
		spawnStarted: make(chan struct{}, 2), spawnRelease: make(chan struct{}, 2),
	}
	monitor := New(sourceWithPullRequests(2), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), mappingPath)
	done := make(chan error, 1)
	go func() {
		_, err := monitor.RunOnce(context.Background())
		done <- err
	}()
	for range 2 {
		select {
		case <-agentHarness.resumeStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for stale resumes")
		}
	}
	close(agentHarness.resumeRelease)
	select {
	case <-agentHarness.spawnStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first spawn")
	}
	agentHarness.spawnRelease <- struct{}{}
	select {
	case <-agentHarness.spawnStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second spawn")
	}
	agentHarness.spawnRelease <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stale-session queue")
	}
	agentHarness.mutex.Lock()
	defer agentHarness.mutex.Unlock()
	if agentHarness.maximumSpawns != 1 {
		t.Fatalf("maximum simultaneous spawns = %d", agentHarness.maximumSpawns)
	}
}

func TestDispatchQueueDefersJobsBeyondCapacity(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	mappingPath := filepath.Join(directory, "mappings.json")
	const expectedPendingLimit = 100
	const pullRequestCount = 101
	source := sourceWithPullRequests(pullRequestCount)
	mappings := &mappingFile{Version: mappingSchemaVersion, PullRequests: make(map[string]mapping, pullRequestCount)}
	for number := 1; number <= pullRequestCount; number++ {
		mappings.PullRequests[fmt.Sprintf("owner/repo#%d", number)] = mapping{Harness: "codex", SessionID: fmt.Sprintf("session-%d", number)}
	}
	if err := saveJSON(mappingPath, mappings); err != nil {
		t.Fatal(err)
	}
	dispatchErrors := make(map[string]error, expectedPendingLimit)
	for number := 1; number <= expectedPendingLimit; number++ {
		dispatchErrors[fmt.Sprintf("session-%d", number)] = context.DeadlineExceeded
	}
	agentHarness := &selectiveHarness{errors: dispatchErrors}
	monitor := New(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), mappingPath)

	result, err := monitor.RunOnce(context.Background())
	if err == nil || result.Dispatches != 0 || result.Deferred != 1 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	agentHarness.mutex.Lock()
	firstAttempts := len(agentHarness.requests)
	agentHarness.mutex.Unlock()
	if firstAttempts != expectedPendingLimit {
		t.Fatalf("first attempts = %d", firstAttempts)
	}
	source.openError = errors.New("temporary GitHub failure")
	result, err = monitor.RunOnce(context.Background())
	if err == nil || result.Dispatches != 0 {
		t.Fatalf("failed-scan result/error = %#v / %v", result, err)
	}
	source.openError = nil
	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 || result.Deferred != maxPendingDispatches {
		t.Fatalf("deferred retry result/error = %#v / %v", result, err)
	}
	agentHarness.mutex.Lock()
	defer agentHarness.mutex.Unlock()
	if agentHarness.requests[len(agentHarness.requests)-1].SessionID != fmt.Sprintf("session-%d", pullRequestCount) {
		t.Fatalf("last request = %#v", agentHarness.requests[len(agentHarness.requests)-1])
	}
}

func TestDispatchQueueCoalescesDuplicatePullRequests(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	source := sourceWithPullRequests(1)
	source.pullRequests = append(source.pullRequests, source.pullRequests[0])
	mappingPath := filepath.Join(directory, "mappings.json")
	if err := saveJSON(mappingPath, &mappingFile{Version: mappingSchemaVersion, PullRequests: map[string]mapping{
		"owner/repo#1": {Harness: "codex", SessionID: "session-1"},
	}}); err != nil {
		t.Fatal(err)
	}
	agentHarness := &selectiveHarness{errors: make(map[string]error)}
	monitor := New(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), mappingPath)

	result, err := monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	agentHarness.mutex.Lock()
	defer agentHarness.mutex.Unlock()
	if len(agentHarness.requests) != 1 {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}
}

func TestLoadMappingsRejectsConflictingCaseVariants(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "mappings.json")
	if err := saveJSON(path, &mappingFile{Version: mappingSchemaVersion, PullRequests: map[string]mapping{
		"Owner/Repo#1": {Harness: "codex", SessionID: "session-1"},
		"owner/repo#1": {Harness: "codex", SessionID: "session-2"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := loadMappings(path); err == nil {
		t.Fatal("expected conflicting mapping error")
	}
}

func TestReviewPromptBoundsUntrustedCommentText(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	threads := make([]githubapi.ReviewThread, 200)
	for index := range threads {
		threads[index] = reviewThread(fmt.Sprintf("large-thread-%d", index), now)
		threads[index].Comments[0].Body = "comment-prefix " + strings.Repeat("界", 2*1024)
	}
	prompt, included := reviewPrompt(githubapi.PullRequest{Repository: "owner/repo", Number: 1}, threads)
	if len(prompt) > 256*1024 || !utf8.ValidString(prompt) || !strings.Contains(prompt, "Additional changed threads omitted") {
		t.Fatalf("bounded prompt length/valid/suffix = %d / %t / %t", len(prompt), utf8.ValidString(prompt), strings.Contains(prompt, "Additional changed threads omitted"))
	}
	if !strings.HasPrefix(prompt, "New or updated unresolved inline review feedback") || !strings.Contains(prompt, "comment-prefix") {
		t.Fatalf("prompt lost required content: %s", prompt[:min(len(prompt), 200)])
	}
	if len(included) == 0 || len(included) >= len(threads) {
		t.Fatalf("included threads = %d", len(included))
	}
}

func TestOmittedThreadsRemainPending(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	threads := make([]githubapi.ReviewThread, 200)
	for index := range threads {
		threads[index] = reviewThread(fmt.Sprintf("large-thread-%d", index), now)
		threads[index].Comments[0].Body = strings.Repeat("feedback ", 400)
	}
	source := &fakeSource{
		pullRequest: githubapi.PullRequest{Repository: "Owner/Repo", Number: 42, URL: "https://example/pr/42"},
		threads:     threads,
	}
	agentHarness := &fakeHarness{}
	statePath := filepath.Join(directory, "state.json")
	monitor := New(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, filepath.Join(directory, "mappings.json"))

	if result, err := monitor.RunOnce(context.Background()); err != nil || result.Dispatches != 1 {
		t.Fatalf("first result/error = %#v / %v", result, err)
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	firstCommitted := len(state.Threads["owner/repo#42"])
	if firstCommitted == 0 || firstCommitted >= len(threads) {
		t.Fatalf("first committed threads = %d", firstCommitted)
	}
	if result, err := monitor.RunOnce(context.Background()); err != nil || result.Dispatches != 1 {
		t.Fatalf("second result/error = %#v / %v", result, err)
	}
	state, err = loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Threads["owner/repo#42"]) <= firstCommitted {
		t.Fatalf("omitted threads did not advance: %#v", state.Threads["owner/repo#42"])
	}
}

func receiveStartedSession(t *testing.T, started <-chan string) string {
	t.Helper()
	select {
	case sessionID := <-started:
		return sessionID
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queued dispatch")
		return ""
	}
}

func sourceWithPullRequests(count int) *fakeSource {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	source := &fakeSource{
		pullRequests:         make([]githubapi.PullRequest, 0, count),
		threadsByPullRequest: make(map[int][]githubapi.ReviewThread, count),
	}
	for number := 1; number <= count; number++ {
		source.pullRequests = append(source.pullRequests, githubapi.PullRequest{
			Repository: "Owner/Repo", Number: number, URL: fmt.Sprintf("https://example/pr/%d", number),
		})
		source.threadsByPullRequest[number] = []githubapi.ReviewThread{reviewThread(fmt.Sprintf("thread-%d", number), now)}
	}
	return source
}

func reviewThread(id string, now time.Time) githubapi.ReviewThread {
	return githubapi.ReviewThread{
		ID: id, Path: "main.go", Comments: []githubapi.ReviewComment{{
			ID: id + "-comment", Author: "reviewer", Body: "consider this",
			URL: "https://example/comment/" + id, CreatedAt: now, UpdatedAt: now,
		}},
	}
}

func reviewSource() *fakeSource {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	return &fakeSource{
		pullRequest: githubapi.PullRequest{Repository: "Owner/Repo", Number: 42, Title: "Feature", URL: "https://example/pr/42"},
		threads:     []githubapi.ReviewThread{reviewThread("thread-1", now)},
	}
}

func readJSON(t *testing.T, path string, output any) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := json.NewDecoder(file).Decode(output); err != nil {
		t.Fatal(err)
	}
}
