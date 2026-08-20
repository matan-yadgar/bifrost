package bridge

import (
	"context"
	"errors"
	"fmt"
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
	pullRequestsByRepo   map[string][]githubapi.PullRequest
	threads              []githubapi.ReviewThread
	threadsByPullRequest map[int][]githubapi.ReviewThread
	threadErrors         map[int]error
	openError            error
	openErrorsByRepo     map[string]error
}

func (source *fakeSource) OpenPullRequests(_ context.Context, repository string, _ []string) ([]githubapi.PullRequest, error) {
	if err := source.openErrorsByRepo[repository]; err != nil {
		return nil, err
	}
	if source.openError != nil {
		return nil, source.openError
	}
	if source.pullRequestsByRepo != nil {
		return source.pullRequestsByRepo[repository], nil
	}
	if source.pullRequests != nil {
		return source.pullRequests, nil
	}
	return []githubapi.PullRequest{source.pullRequest}, nil
}

func (source *fakeSource) ReviewThreads(_ context.Context, pullRequest githubapi.PullRequest) ([]githubapi.ReviewThread, error) {
	if err := source.threadErrors[pullRequest.Number]; err != nil {
		return nil, err
	}
	if source.threadsByPullRequest != nil {
		return source.threadsByPullRequest[pullRequest.Number], nil
	}
	return source.threads, nil
}

type fakeHarness struct {
	mutex         sync.Mutex
	name          string
	requests      []harness.Request
	targets       []harness.Target
	discoverCalls int
	discoveredID  string
	discoverErr   error
	discover      func([]harness.Target) ([]harness.Discovery, error)
	startedID     string
	err           error
	dispatch      func(harness.Request) (harness.Result, error)
}

func newTestMonitor(source Source, agentHarness harness.Harness, repositories []Repository, statePath string, routes map[string]route) *Monitor {
	state, err := loadState(statePath)
	if err != nil {
		panic(err)
	}
	if len(routes) > 0 {
		for key, value := range routes {
			state.Routes[key] = value
		}
		if err := saveJSON(statePath, state); err != nil {
			panic(err)
		}
	}
	return New(source, agentHarness, repositories, statePath, time.Minute)
}

func readTestRoute(t *testing.T, statePath, key string) route {
	t.Helper()
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	return state.Routes[key]
}

func (agentHarness *fakeHarness) Name() string {
	if agentHarness.name != "" {
		return agentHarness.name
	}
	return "codex"
}

func (agentHarness *fakeHarness) Discover(_ context.Context, targets []harness.Target) ([]harness.Discovery, error) {
	agentHarness.discoverCalls++
	agentHarness.targets = append(agentHarness.targets, targets...)
	if agentHarness.discover != nil {
		return agentHarness.discover(targets)
	}
	discoveries := make([]harness.Discovery, len(targets))
	for index := range discoveries {
		if agentHarness.discoverErr != nil {
			discoveries[index].Err = agentHarness.discoverErr
		} else if agentHarness.discoveredID != "" {
			discoveries[index] = harness.Discovery{Session: harness.Session{ID: agentHarness.discoveredID}, Found: true}
		}
	}
	return discoveries, nil
}

func (agentHarness *fakeHarness) Dispatch(_ context.Context, request harness.Request) (harness.Result, error) {
	agentHarness.mutex.Lock()
	agentHarness.requests = append(agentHarness.requests, request)
	dispatch := agentHarness.dispatch
	dispatchError := agentHarness.err
	if request.SessionID == "" && agentHarness.startedID == "" {
		agentHarness.startedID = "019c0000-0000-7000-8000-000000000010"
	}
	startedID := agentHarness.startedID
	agentHarness.mutex.Unlock()
	if dispatch != nil {
		return dispatch(request)
	}
	if request.SessionID != "" {
		return harness.Result{SessionID: request.SessionID}, dispatchError
	}
	return harness.Result{SessionID: startedID}, dispatchError
}

func TestMonitorDispatchesChangedUnresolvedThreads(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)
	source := &fakeSource{
		pullRequest: githubapi.PullRequest{Repository: "Owner/Repo", Number: 42, Title: "Feature", URL: "https://example/pr/42", HeadRef: "codex/feature-42"},
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
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, nil)

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
	if !strings.Contains(agentHarness.requests[0].Prompt, "Head branch: codex/feature-42") {
		t.Fatalf("prompt did not identify the PR head branch: %s", agentHarness.requests[0].Prompt)
	}
	if value := readTestRoute(t, statePath, "owner/repo#42"); value.SessionID != agentHarness.startedID {
		t.Fatalf("route = %#v", value)
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
		t.Fatalf("updated thread did not resume cached session: %#v", agentHarness.requests[1])
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

func TestMonitorDiscoversExistingTaskBeforeDispatch(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	const sessionID = "019c0000-0000-7000-8000-000000000019"
	agentHarness := &fakeHarness{discoveredID: sessionID}
	statePath := filepath.Join(directory, "state.json")
	source := reviewSource()
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, nil)

	result, err := monitor.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Dispatches != 1 || len(agentHarness.requests) != 1 || agentHarness.requests[0].SessionID != sessionID {
		t.Fatalf("result/requests = %#v / %#v", result, agentHarness.requests)
	}
	if len(agentHarness.targets) != 1 {
		t.Fatalf("discovery targets = %#v", agentHarness.targets)
	}
	target := agentHarness.targets[0]
	if target.Repository != source.pullRequest.Repository || target.PullRequest != source.pullRequest.Number || target.URL != source.pullRequest.URL || target.HeadRef != source.pullRequest.HeadRef || target.WorkingDirectory != directory {
		t.Fatalf("discovery target = %#v", target)
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.Routes["owner/repo#42"].SessionID != sessionID {
		t.Fatalf("routes = %#v", state.Routes)
	}
}

func TestMonitorDiscoversUnroutedJobsInOneBatch(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	const discoveredSessionID = "019c0000-0000-7000-8000-000000000020"
	const spawnedSessionID = "019c0000-0000-7000-8000-000000000021"
	discoveryFailure := errors.New("target discovery failed")
	agentHarness := &fakeHarness{
		startedID: spawnedSessionID,
		discover: func(targets []harness.Target) ([]harness.Discovery, error) {
			if len(targets) != 3 || targets[0].PullRequest != 1 || targets[1].PullRequest != 2 || targets[2].PullRequest != 3 {
				return nil, fmt.Errorf("unexpected targets: %#v", targets)
			}
			return []harness.Discovery{
				{Session: harness.Session{ID: discoveredSessionID}, Found: true},
				{},
				{Err: discoveryFailure},
			}, nil
		},
	}
	monitor := newTestMonitor(sourceWithPullRequests(3), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, nil)

	result, err := monitor.RunOnce(context.Background())
	if !errors.Is(err, discoveryFailure) || result.Dispatches != 2 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	if agentHarness.discoverCalls != 1 || len(agentHarness.targets) != 3 {
		t.Fatalf("discovery calls/targets = %d / %#v", agentHarness.discoverCalls, agentHarness.targets)
	}
	if len(agentHarness.requests) != 2 {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}
	requestedSessions := map[string]bool{}
	for _, request := range agentHarness.requests {
		requestedSessions[request.SessionID] = true
	}
	if !requestedSessions[discoveredSessionID] || !requestedSessions[""] {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}
	state, loadError := loadState(statePath)
	if loadError != nil {
		t.Fatal(loadError)
	}
	if state.Routes["owner/repo#1"].SessionID != discoveredSessionID || state.Routes["owner/repo#2"].SessionID != spawnedSessionID || state.Routes["owner/repo#3"].SessionID != "" {
		t.Fatalf("routes = %#v", state.Routes)
	}
	if state.Threads["owner/repo#1"] == nil || state.Threads["owner/repo#2"] == nil || state.Threads["owner/repo#3"] != nil {
		t.Fatalf("threads = %#v", state.Threads)
	}
}

func TestMonitorRotatesPastDeferredDiscoverySuffix(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	firstFailure := errors.New("fatal discovery failure")
	secondFailure := errors.New("stop after rotation check")
	discoverCalls := 0
	agentHarness := &fakeHarness{discover: func(targets []harness.Target) ([]harness.Discovery, error) {
		discoverCalls++
		discoveries := make([]harness.Discovery, len(targets))
		if discoverCalls == 1 {
			discoveries[0].Err = firstFailure
			for index := 1; index < len(discoveries); index++ {
				discoveries[index].Err = fmt.Errorf("%w: reconnect budget exhausted", harness.ErrDiscoveryDeferred)
			}
			return discoveries, nil
		}
		for index := range discoveries {
			discoveries[index].Err = secondFailure
		}
		return discoveries, nil
	}}
	monitor := newTestMonitor(sourceWithPullRequests(6), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), nil)

	if _, err := monitor.RunOnce(context.Background()); !errors.Is(err, harness.ErrDiscoveryDeferred) {
		t.Fatalf("first discovery error = %v", err)
	}
	if _, err := monitor.RunOnce(context.Background()); !errors.Is(err, secondFailure) {
		t.Fatalf("second discovery error = %v", err)
	}
	if len(agentHarness.targets) != 12 || agentHarness.targets[6].PullRequest != 2 {
		t.Fatalf("discovery order = %#v", agentHarness.targets)
	}
}

func TestMonitorReplacesRouteOwnedByAnotherHarness(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	const discoveredSessionID = "019c0000-0000-7000-8000-000000000021"
	agentHarness := &fakeHarness{discoveredID: discoveredSessionID}
	routes := map[string]route{"owner/repo#42": {Harness: "claude", SessionID: "claude-session"}}
	monitor := newTestMonitor(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)

	result, err := monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	if len(agentHarness.requests) != 1 || agentHarness.requests[0].SessionID != discoveredSessionID {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}
	if value := readTestRoute(t, statePath, "owner/repo#42"); value.Harness != "codex" || value.SessionID != discoveredSessionID {
		t.Fatalf("route = %#v", value)
	}
}

func TestMonitorLeavesFeedbackPendingWhenDiscoveryIsAmbiguous(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	agentHarness := &fakeHarness{discoverErr: harness.ErrAmbiguousSession}
	statePath := filepath.Join(directory, "state.json")
	monitor := newTestMonitor(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, nil)

	result, err := monitor.RunOnce(context.Background())
	if !errors.Is(err, harness.ErrAmbiguousSession) || result.Dispatches != 0 || len(agentHarness.requests) != 0 {
		t.Fatalf("result/error/requests = %#v / %v / %#v", result, err, agentHarness.requests)
	}
	state, loadError := loadState(statePath)
	if loadError != nil {
		t.Fatal(loadError)
	}
	if len(state.Threads) != 0 || len(state.Routes) != 0 {
		t.Fatalf("ambiguous discovery changed delivery state: %#v", state)
	}
}

func TestMonitorPrunesClosedPullRequestStateAfterSuccessfulListing(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	openPullRequest := reviewSource().pullRequest
	openThread := reviewThread("open-thread", time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC))
	if err := saveJSON(statePath, &stateFile{
		Version: stateSchemaVersion,
		Threads: map[string]map[string]string{
			"owner/repo#42": {openThread.ID: fingerprint(openThread)},
			"owner/repo#41": {"closed-thread": "fingerprint"},
		},
		Routes: map[string]route{
			"owner/repo#42": {Harness: "codex", SessionID: "open-session"},
			"owner/repo#41": {Harness: "codex", SessionID: "closed-session"},
			"other/repo#1":  {Harness: "codex", SessionID: "other-session"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	source := &fakeSource{pullRequest: openPullRequest, threads: []githubapi.ReviewThread{openThread}}
	monitor := New(source, &fakeHarness{}, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, time.Minute)

	result, err := monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 0 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.Threads["owner/repo#41"] != nil || state.Routes["owner/repo#41"].SessionID != "" {
		t.Fatalf("closed PR state remains: %#v", state)
	}
	if state.Threads["owner/repo#42"] == nil || state.Routes["owner/repo#42"].SessionID != "open-session" || state.Routes["other/repo#1"].SessionID != "other-session" {
		t.Fatalf("active or unrelated state was pruned: %#v", state)
	}
}

func TestMonitorDoesNotPruneRoutesWhenPullRequestListingFails(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	if err := saveJSON(statePath, &stateFile{
		Version: stateSchemaVersion,
		Threads: map[string]map[string]string{"owner/repo#41": {"thread": "fingerprint"}},
		Routes:  map[string]route{"owner/repo#41": {Harness: "codex", SessionID: "preserved-session"}},
	}); err != nil {
		t.Fatal(err)
	}
	monitor := New(&fakeSource{openError: errors.New("temporary failure")}, &fakeHarness{}, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, time.Minute)

	if _, err := monitor.RunOnce(context.Background()); err == nil {
		t.Fatal("expected listing error")
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.Routes["owner/repo#41"].SessionID != "preserved-session" || state.Threads["owner/repo#41"]["thread"] != "fingerprint" {
		t.Fatalf("state was pruned after failed listing: %#v", state)
	}
}

func TestMonitorPrunesSuccessfulRepositoryAndPreservesFailedRepository(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	if err := saveJSON(statePath, &stateFile{
		Version: stateSchemaVersion,
		Threads: map[string]map[string]string{
			"good/repo#1": {"thread": "fingerprint"},
			"bad/repo#2":  {"thread": "fingerprint"},
		},
		Routes: map[string]route{
			"good/repo#1": {Harness: "codex", SessionID: "good-session"},
			"bad/repo#2":  {Harness: "codex", SessionID: "bad-session"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	source := &fakeSource{
		pullRequestsByRepo: map[string][]githubapi.PullRequest{"Good/Repo": nil},
		openErrorsByRepo:   map[string]error{"Bad/Repo": errors.New("temporary failure")},
	}
	monitor := New(source, &fakeHarness{}, []Repository{
		{Name: "Good/Repo", WorkingDirectory: directory},
		{Name: "Bad/Repo", WorkingDirectory: directory},
	}, statePath, time.Minute)

	if _, err := monitor.RunOnce(context.Background()); err == nil {
		t.Fatal("expected partial listing error")
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.Routes["good/repo#1"].SessionID != "" || state.Threads["good/repo#1"] != nil {
		t.Fatalf("successfully scanned repository was not pruned: %#v", state)
	}
	if state.Routes["bad/repo#2"].SessionID != "bad-session" || state.Threads["bad/repo#2"]["thread"] != "fingerprint" {
		t.Fatalf("failed repository state was not preserved: %#v", state)
	}
}

func TestMonitorPersistsStartedSessionAndRetriesFailedDelivery(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	source := reviewSource()
	dispatchError := context.DeadlineExceeded
	agentHarness := &fakeHarness{
		startedID: "019c0000-0000-7000-8000-000000000011",
		err:       dispatchError,
	}
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, nil)

	result, err := monitor.RunOnce(context.Background())
	if err == nil || result.Dispatches != 0 {
		t.Fatalf("failed result/error = %#v / %v", result, err)
	}
	startedRoute := readTestRoute(t, statePath, "owner/repo#42")
	if startedRoute.SessionID != agentHarness.startedID {
		t.Fatalf("route = %#v", startedRoute)
	}

	agentHarness.err = nil
	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("retry result/error = %#v / %v", result, err)
	}
	if agentHarness.requests[1].SessionID != agentHarness.startedID {
		t.Fatalf("retry did not resume started session: %#v", agentHarness.requests[1])
	}
}

func TestMonitorTreatsBlankCachedSessionAsUnrouted(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	routes := map[string]route{"owner/repo#42": {Harness: "codex", SessionID: "  "}}
	agentHarness := &fakeHarness{startedID: "019c0000-0000-7000-8000-000000000013"}
	monitor := newTestMonitor(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)

	if _, err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 1 || agentHarness.requests[0].SessionID != "" {
		t.Fatalf("request = %#v", agentHarness.requests)
	}
	if value := readTestRoute(t, statePath, "owner/repo#42"); value.SessionID != agentHarness.startedID {
		t.Fatalf("route = %#v", value)
	}
}

func TestMonitorReplacesRediscoveredMissingCachedSession(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	const oldSessionID = "019c0000-0000-7000-8000-000000000014"
	const newSessionID = "019c0000-0000-7000-8000-000000000015"
	routes := map[string]route{
		"owner/repo#42": {Harness: "codex", SessionID: oldSessionID},
	}
	agentHarness := &fakeHarness{discoveredID: oldSessionID, dispatch: func(request harness.Request) (harness.Result, error) {
		if request.SessionID == oldSessionID {
			return harness.Result{SessionID: oldSessionID}, harness.ErrSessionNotFound
		}
		if request.SessionID != "" {
			return harness.Result{}, fmt.Errorf("unexpected session ID %q", request.SessionID)
		}
		return harness.Result{SessionID: newSessionID}, nil
	}}
	monitor := newTestMonitor(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)

	result, err := monitor.RunOnce(context.Background())
	if !errors.Is(err, harness.ErrSessionNotFound) || result.Dispatches != 0 {
		t.Fatalf("stale result/error = %#v / %v", result, err)
	}
	staleRoute := readTestRoute(t, statePath, "owner/repo#42")
	if len(agentHarness.requests) != 1 || agentHarness.requests[0].SessionID != oldSessionID || staleRoute.SessionID != oldSessionID || !staleRoute.Stale {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}

	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("replacement result/error = %#v / %v", result, err)
	}
	if len(agentHarness.requests) != 2 || agentHarness.requests[1].SessionID != "" {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}
	if value := readTestRoute(t, statePath, "owner/repo#42"); value.SessionID != newSessionID {
		t.Fatalf("route = %#v", value)
	}

	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 0 || len(agentHarness.requests) != 2 {
		t.Fatalf("unchanged result/error/requests = %#v / %v / %#v", result, err, agentHarness.requests)
	}
}

func TestMonitorRediscoversTaskWhenCachedSessionIsStale(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	const oldSessionID = "019c0000-0000-7000-8000-000000000016"
	const rediscoveredSessionID = "019c0000-0000-7000-8000-000000000017"
	routes := map[string]route{
		"owner/repo#42": {Harness: "codex", SessionID: oldSessionID},
	}
	agentHarness := &fakeHarness{
		discoveredID: rediscoveredSessionID,
		dispatch: func(request harness.Request) (harness.Result, error) {
			if request.SessionID == oldSessionID {
				return harness.Result{SessionID: oldSessionID}, harness.ErrSessionNotFound
			}
			return harness.Result{SessionID: request.SessionID}, nil
		},
	}
	monitor := newTestMonitor(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)

	result, err := monitor.RunOnce(context.Background())
	if !errors.Is(err, harness.ErrSessionNotFound) || result.Dispatches != 0 {
		t.Fatalf("stale result/error = %#v / %v", result, err)
	}
	if len(agentHarness.requests) != 1 || agentHarness.requests[0].SessionID != oldSessionID {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}

	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("rediscovery result/error = %#v / %v", result, err)
	}
	if len(agentHarness.requests) != 2 || agentHarness.requests[1].SessionID != rediscoveredSessionID {
		t.Fatalf("requests = %#v", agentHarness.requests)
	}
	if value := readTestRoute(t, statePath, "owner/repo#42"); value.SessionID != rediscoveredSessionID {
		t.Fatalf("route = %#v", value)
	}
}

func TestMonitorLeavesStaleRouteFeedbackPendingWhenRediscoveryFails(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	const staleSessionID = "019c0000-0000-7000-8000-000000000018"
	routes := map[string]route{"owner/repo#42": {Harness: "codex", SessionID: staleSessionID}}
	agentHarness := &fakeHarness{
		discoverErr: harness.ErrAmbiguousSession,
		dispatch: func(request harness.Request) (harness.Result, error) {
			return harness.Result{SessionID: request.SessionID}, harness.ErrSessionNotFound
		},
	}
	monitor := newTestMonitor(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)

	result, err := monitor.RunOnce(context.Background())
	if !errors.Is(err, harness.ErrSessionNotFound) || result.Dispatches != 0 || len(agentHarness.requests) != 1 || agentHarness.discoverCalls != 0 {
		t.Fatalf("first result/error/requests = %#v / %v / %#v", result, err, agentHarness.requests)
	}
	state, loadError := loadState(statePath)
	if loadError != nil {
		t.Fatal(loadError)
	}
	if current := state.Routes["owner/repo#42"]; current.SessionID != staleSessionID || !current.Stale || len(state.Threads) != 0 {
		t.Fatalf("failed rediscovery committed state: %#v", state)
	}

	result, err = monitor.RunOnce(context.Background())
	if !errors.Is(err, harness.ErrAmbiguousSession) || result.Dispatches != 0 || len(agentHarness.requests) != 1 || agentHarness.discoverCalls != 1 {
		t.Fatalf("retry result/error/requests/discoveries = %#v / %v / %#v / %d", result, err, agentHarness.requests, agentHarness.discoverCalls)
	}
	result, err = monitor.RunOnce(context.Background())
	if !errors.Is(err, harness.ErrAmbiguousSession) || result.Dispatches != 0 || len(agentHarness.requests) != 1 || agentHarness.discoverCalls != 2 {
		t.Fatalf("second retry result/error/requests/discoveries = %#v / %v / %#v / %d", result, err, agentHarness.requests, agentHarness.discoverCalls)
	}
}

func TestMonitorRetriesStaleRouteAfterBatchDiscoveryFailure(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	const staleSessionID = "019c0000-0000-7000-8000-000000000030"
	const replacementSessionID = "019c0000-0000-7000-8000-000000000031"
	discoveryUnavailable := errors.New("discovery unavailable")
	replacementFailed := errors.New("replacement failed")
	unavailable := true
	spawnFails := true
	agentHarness := &fakeHarness{
		discover: func(targets []harness.Target) ([]harness.Discovery, error) {
			if unavailable {
				return nil, discoveryUnavailable
			}
			return make([]harness.Discovery, len(targets)), nil
		},
		dispatch: func(request harness.Request) (harness.Result, error) {
			if request.SessionID == staleSessionID {
				return harness.Result{SessionID: staleSessionID}, harness.ErrSessionNotFound
			}
			if spawnFails {
				return harness.Result{}, replacementFailed
			}
			return harness.Result{SessionID: replacementSessionID}, nil
		},
	}
	monitor := newTestMonitor(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, map[string]route{
		"owner/repo#42": {Harness: "codex", SessionID: staleSessionID},
	})

	if result, err := monitor.RunOnce(context.Background()); !errors.Is(err, harness.ErrSessionNotFound) || result.Dispatches != 0 {
		t.Fatalf("stale result/error = %#v / %v", result, err)
	}
	if result, err := monitor.RunOnce(context.Background()); !errors.Is(err, discoveryUnavailable) || result.Dispatches != 0 {
		t.Fatalf("unavailable result/error = %#v / %v", result, err)
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if current := state.Routes["owner/repo#42"]; current.SessionID != staleSessionID || !current.Stale || state.Threads["owner/repo#42"] != nil {
		t.Fatalf("pending stale state = %#v", state)
	}

	unavailable = false
	if result, err := monitor.RunOnce(context.Background()); !errors.Is(err, replacementFailed) || result.Dispatches != 0 {
		t.Fatalf("failed replacement result/error = %#v / %v", result, err)
	}
	if current := readTestRoute(t, statePath, "owner/repo#42"); current.SessionID != staleSessionID || !current.Stale {
		t.Fatalf("stale route was lost after failed replacement: %#v", current)
	}
	spawnFails = false
	if result, err := monitor.RunOnce(context.Background()); err != nil || result.Dispatches != 1 {
		t.Fatalf("recovery result/error = %#v / %v", result, err)
	}
	if current := readTestRoute(t, statePath, "owner/repo#42"); current.SessionID != replacementSessionID || current.Stale {
		t.Fatalf("replacement route = %#v", current)
	}
}

func TestMonitorDoesNotCommitDeliveryWithoutSessionID(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	agentHarness := &fakeHarness{dispatch: func(harness.Request) (harness.Result, error) {
		return harness.Result{}, nil
	}}
	monitor := newTestMonitor(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), nil)

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

func (agentHarness *blockingHarness) Discover(_ context.Context, targets []harness.Target) ([]harness.Discovery, error) {
	return make([]harness.Discovery, len(targets)), nil
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
	const sharedSessionID = "019c0000-0000-7000-8000-000000000020"
	const otherSessionID = "019c0000-0000-7000-8000-000000000021"
	const thirdSessionID = "019c0000-0000-7000-8000-000000000022"
	routes := map[string]route{
		"owner/repo#1": {Harness: "codex", SessionID: sharedSessionID},
		"owner/repo#2": {Harness: "codex", SessionID: sharedSessionID},
		"owner/repo#3": {Harness: "codex", SessionID: otherSessionID},
		"owner/repo#4": {Harness: "codex", SessionID: thirdSessionID},
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
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), routes)
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
	routes := make(map[string]route)
	for number := 1; number <= 5; number++ {
		routes[fmt.Sprintf("owner/repo#%d", number)] = route{Harness: "codex", SessionID: fmt.Sprintf("session-%d", number)}
	}
	source := sourceWithPullRequests(5)
	agentHarness := &blockingHarness{started: make(chan string, 5), release: make(chan struct{})}
	statePath := filepath.Join(directory, "state.json")
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)
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

type timeoutHarness struct {
	sessionID string
	deadline  chan timeoutObservation
}

type timeoutObservation struct {
	observedAt time.Time
	deadline   time.Time
}

func (agentHarness *timeoutHarness) Name() string {
	return "codex"
}

func (agentHarness *timeoutHarness) Discover(_ context.Context, targets []harness.Target) ([]harness.Discovery, error) {
	return make([]harness.Discovery, len(targets)), nil
}

func (agentHarness *timeoutHarness) Dispatch(ctx context.Context, _ harness.Request) (harness.Result, error) {
	deadline, found := ctx.Deadline()
	if !found {
		return harness.Result{}, errors.New("dispatch context has no deadline")
	}
	agentHarness.deadline <- timeoutObservation{observedAt: time.Now(), deadline: deadline}
	<-ctx.Done()
	return harness.Result{SessionID: agentHarness.sessionID}, ctx.Err()
}

func TestDispatchTimeoutPreservesSessionAndLeavesFeedbackPending(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	const dispatchTimeout = 250 * time.Millisecond
	agentHarness := &timeoutHarness{sessionID: "started-session", deadline: make(chan timeoutObservation, 1)}
	statePath := filepath.Join(directory, "state.json")
	monitor := New(reviewSource(), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, dispatchTimeout)
	type cycleOutcome struct {
		result CycleResult
		err    error
	}
	completed := make(chan cycleOutcome, 1)
	go func() {
		result, err := monitor.RunOnce(context.Background())
		completed <- cycleOutcome{result: result, err: err}
	}()
	select {
	case observation := <-agentHarness.deadline:
		configured := observation.deadline.Sub(observation.observedAt)
		if configured < dispatchTimeout-100*time.Millisecond || configured > dispatchTimeout+100*time.Millisecond {
			t.Fatalf("dispatch deadline = %s after start", configured)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dispatch did not start")
	}
	var outcome cycleOutcome
	select {
	case outcome = <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("timed-out dispatch did not return")
	}
	result, err := outcome.result, outcome.err
	if !errors.Is(err, context.DeadlineExceeded) || result.Dispatches != 0 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	if value := readTestRoute(t, statePath, "owner/repo#42"); value.SessionID != agentHarness.sessionID {
		t.Fatalf("route = %#v", value)
	}
	state, err := loadState(filepath.Join(directory, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Threads) != 0 {
		t.Fatalf("timed-out feedback was committed: %#v", state.Threads)
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

func (agentHarness *selectiveHarness) Discover(_ context.Context, targets []harness.Target) ([]harness.Discovery, error) {
	return make([]harness.Discovery, len(targets)), nil
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
	routes := map[string]route{
		"owner/repo#1": {Harness: "codex", SessionID: "session-success"},
		"owner/repo#2": {Harness: "codex", SessionID: "session-failure"},
	}
	agentHarness := &selectiveHarness{errors: map[string]error{"session-failure": context.DeadlineExceeded}}
	statePath := filepath.Join(directory, "state.json")
	monitor := newTestMonitor(sourceWithPullRequests(2), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)

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

func TestPerPullRequestSourceFailureDoesNotBlockHealthyDispatch(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	routes := map[string]route{
		"owner/repo#1": {Harness: "codex", SessionID: "session-1"},
		"owner/repo#2": {Harness: "codex", SessionID: "session-2"},
	}
	source := sourceWithPullRequests(2)
	source.threadErrors = map[int]error{1: errors.New("temporary thread failure")}
	agentHarness := &selectiveHarness{errors: make(map[string]error)}
	statePath := filepath.Join(directory, "state.json")
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)

	result, err := monitor.RunOnce(context.Background())
	if err == nil || result.Dispatches != 1 {
		t.Fatalf("first result/error = %#v / %v", result, err)
	}
	state, err := loadState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if state.Threads["owner/repo#1"] != nil || state.Threads["owner/repo#2"] == nil {
		t.Fatalf("state = %#v", state.Threads)
	}
	delete(source.threadErrors, 1)
	result, err = monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != 1 {
		t.Fatalf("retry result/error = %#v / %v", result, err)
	}
	agentHarness.mutex.Lock()
	defer agentHarness.mutex.Unlock()
	if len(agentHarness.requests) != 2 || agentHarness.requests[1].SessionID != "session-1" {
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

func (agentHarness *staleSessionHarness) Discover(_ context.Context, targets []harness.Target) ([]harness.Discovery, error) {
	return make([]harness.Discovery, len(targets)), nil
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
	routes := map[string]route{
		"owner/repo#1": {Harness: "codex", SessionID: "stale-1"},
		"owner/repo#2": {Harness: "codex", SessionID: "stale-2"},
	}
	agentHarness := &staleSessionHarness{
		resumeStarted: make(chan struct{}, 2), resumeRelease: make(chan struct{}),
		spawnStarted: make(chan struct{}, 2), spawnRelease: make(chan struct{}, 2),
	}
	monitor := newTestMonitor(sourceWithPullRequests(2), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), routes)
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
	case err := <-done:
		if !errors.Is(err, harness.ErrSessionNotFound) {
			t.Fatalf("stale-route error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out clearing stale routes")
	}
	go func() {
		_, err := monitor.RunOnce(context.Background())
		done <- err
	}()
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
	const expectedPendingLimit = 100
	const pullRequestCount = 101
	source := sourceWithPullRequests(pullRequestCount)
	routes := make(map[string]route, pullRequestCount)
	for number := 1; number <= pullRequestCount; number++ {
		routes[fmt.Sprintf("owner/repo#%d", number)] = route{Harness: "codex", SessionID: fmt.Sprintf("session-%d", number)}
	}
	dispatchErrors := make(map[string]error, expectedPendingLimit)
	for number := 1; number <= expectedPendingLimit; number++ {
		dispatchErrors[fmt.Sprintf("session-%d", number)] = context.DeadlineExceeded
	}
	agentHarness := &selectiveHarness{errors: dispatchErrors}
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), routes)

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
	if err == nil || result.Dispatches != 1 || result.Deferred != 1 {
		t.Fatalf("deferred retry result/error = %#v / %v", result, err)
	}
	agentHarness.mutex.Lock()
	defer agentHarness.mutex.Unlock()
	foundDeferred := false
	for _, request := range agentHarness.requests[firstAttempts:] {
		foundDeferred = foundDeferred || request.SessionID == fmt.Sprintf("session-%d", pullRequestCount)
	}
	if !foundDeferred {
		t.Fatalf("deferred session was not attempted: %#v", agentHarness.requests[firstAttempts:])
	}
}

func TestDispatchQueueWrapsWhenBacklogShrinksBelowCursor(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	statePath := filepath.Join(directory, "state.json")
	if err := saveJSON(statePath, &stateFile{Version: stateSchemaVersion, QueueCursor: 100, Threads: make(map[string]map[string]string)}); err != nil {
		t.Fatal(err)
	}
	const pullRequestCount = 50
	routes := make(map[string]route, pullRequestCount)
	for number := 1; number <= pullRequestCount; number++ {
		routes[fmt.Sprintf("owner/repo#%d", number)] = route{Harness: "codex", SessionID: fmt.Sprintf("session-%d", number)}
	}
	agentHarness := &selectiveHarness{errors: make(map[string]error)}
	monitor := newTestMonitor(sourceWithPullRequests(pullRequestCount), agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, routes)

	result, err := monitor.RunOnce(context.Background())
	if err != nil || result.Dispatches != pullRequestCount || result.Deferred != 0 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
}

func TestDispatchQueueCoalescesDuplicatePullRequests(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	source := sourceWithPullRequests(1)
	source.pullRequests = append(source.pullRequests, source.pullRequests[0])
	routes := map[string]route{
		"owner/repo#1": {Harness: "codex", SessionID: "session-1"},
	}
	agentHarness := &selectiveHarness{errors: make(map[string]error)}
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, filepath.Join(directory, "state.json"), routes)

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
	monitor := newTestMonitor(source, agentHarness, []Repository{{Name: "Owner/Repo", WorkingDirectory: directory}}, statePath, nil)

	if result, err := monitor.RunOnce(context.Background()); err != nil || result.Dispatches != 1 || result.DeferredThreads == 0 {
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
			Repository: "Owner/Repo", Number: number, URL: fmt.Sprintf("https://example/pr/%d", number), HeadRef: fmt.Sprintf("codex/feature-%d", number),
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
		pullRequest: githubapi.PullRequest{Repository: "Owner/Repo", Number: 42, Title: "Feature", URL: "https://example/pr/42", HeadRef: "codex/feature-42"},
		threads:     []githubapi.ReviewThread{reviewThread("thread-1", now)},
	}
}
