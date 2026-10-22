package bridge

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	githubapi "github.com/matan-yadgar/bifrost/internal/github"
	"github.com/matan-yadgar/bifrost/internal/harness"
)

func ciFixture(t *testing.T) (*Monitor, *fakeSource, *fakeHarness) {
	t.Helper()
	source := &fakeSource{pullRequest: githubapi.PullRequest{Repository: "owner/repo", Number: 42, URL: "https://github.com/owner/repo/pull/42", HeadSHA: "head-1"}, checks: []githubapi.FailedCheck{{Name: "unit", URL: "https://github.com/owner/repo/actions/runs/1", Conclusion: "failure"}}}
	agent := &fakeHarness{discoveredID: "019c0000-0000-7000-8000-000000000003"}
	monitor := New(source, agent, []Repository{{Name: "owner/repo", WorkingDirectory: t.TempDir()}}, filepath.Join(t.TempDir(), "state.json"), time.Minute, true, discardLogger())
	return monitor, source, agent
}

func TestCIAndCommentsUseOneCachedForkAndPersistentFailuresStayEligible(t *testing.T) {
	t.Parallel()
	monitor, source, agent := ciFixture(t)
	source.threads = []githubapi.ReviewThread{reviewThread("review", time.Now())}
	run := func(want int) {
		t.Helper()
		result, err := monitor.RunOnce(context.Background())
		if err != nil || result.Dispatches != want {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	run(1)
	if len(agent.requests) != 1 || !strings.Contains(agent.requests[0].Prompt, "review feedback") || !strings.Contains(agent.requests[0].Prompt, "CI failures") {
		t.Fatalf("requests=%+v", agent.requests)
	}
	run(1)
	source.pullRequest.HeadSHA = "head-2"
	run(1)
	if agent.discoverCalls != 1 {
		t.Fatalf("created additional forks: %d discoveries", agent.discoverCalls)
	}
	for _, request := range agent.requests {
		if request.SessionID != agent.discoveredID {
			t.Fatal("CI did not use the review fork")
		}
	}
}

func TestFailedCIDeliveryRetriesAndDisabledMonitoringDoesNotFetchCI(t *testing.T) {
	t.Parallel()
	monitor, source, agent := ciFixture(t)
	agent.err = errors.New("temporary dispatch failure")
	if _, err := monitor.RunOnce(context.Background()); err == nil {
		t.Fatal("dispatch failure lost")
	}
	agent.err = nil
	if r, err := monitor.RunOnce(context.Background()); err != nil || r.Dispatches != 1 {
		t.Fatalf("retry=%+v err=%v", r, err)
	}
	monitor.monitorCI = false
	source.checksError = errors.New("CI must not be read")
	if _, err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPauseWrittenDuringDeliverySurvivesSaveAndNewHeads(t *testing.T) {
	t.Parallel()
	monitor, source, agent := ciFixture(t)
	agent.dispatch = func(request harness.Request) (harness.Result, error) {
		lock, err := monitor.stateLock(context.Background())
		if err != nil {
			return harness.Result{}, err
		}
		defer func() { _ = lock.Close() }()
		latest, err := loadState(monitor.stateFile)
		if err != nil {
			return harness.Result{}, err
		}
		saved := latest.CI["owner/repo#42"]
		saved.Paused = true
		saved.Reason = "external outage"
		saved.ResumeCondition = "service recovered"
		if err := saveJSON(monitor.stateFile, latest); err != nil {
			return harness.Result{}, err
		}
		return harness.Result{SessionID: request.SessionID}, nil
	}
	if _, err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.pullRequest.HeadSHA = "new-head"
	if r, err := monitor.RunOnce(context.Background()); err != nil || r.Dispatches != 0 {
		t.Fatalf("paused=%+v err=%v", r, err)
	}
	latest, err := loadState(monitor.stateFile)
	if err != nil {
		t.Fatal(err)
	}
	saved := latest.CI["owner/repo#42"]
	if !saved.Paused || saved.Reason != "external outage" || saved.ResumeCondition != "service recovered" {
		t.Fatalf("pause overwritten: %+v", saved)
	}
	// Review feedback still goes to the same fork while automatic CI is paused.
	source.threads = []githubapi.ReviewThread{reviewThread("new-review", time.Now())}
	agent.dispatch = nil
	if r, err := monitor.RunOnce(context.Background()); err != nil || r.Dispatches != 1 {
		t.Fatalf("review=%+v err=%v", r, err)
	}
	if strings.Contains(agent.requests[len(agent.requests)-1].Prompt, "CI failures") {
		t.Fatal("CI included despite pause")
	}
	lock, err := monitor.stateLock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	latest, err = loadState(monitor.stateFile)
	if err != nil {
		t.Fatal(err)
	}
	latest.CI["owner/repo#42"].Paused = false
	err = saveJSON(monitor.stateFile, latest)
	_ = lock.Close()
	if err != nil {
		t.Fatal(err)
	}
	if r, err := monitor.RunOnce(context.Background()); err != nil || r.Dispatches != 1 {
		t.Fatalf("resume=%+v err=%v", r, err)
	}
}

func TestDispatchRereadsPauseAfterQueueingAndClosurePrunesState(t *testing.T) {
	t.Parallel()
	monitor, source, agent := ciFixture(t)
	state, err := loadState(monitor.stateFile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = monitor.changedChecks(context.Background(), state, source.pullRequest)
	if err != nil {
		t.Fatal(err)
	}
	state.CI["owner/repo#42"].Paused = true
	if err := saveJSON(monitor.stateFile, state); err != nil {
		t.Fatal(err)
	}
	outcome := monitor.dispatch(context.Background(), queuedDispatch{job: dispatchJob{key: "owner/repo#42", repository: monitor.repositories[0], pullRequest: source.pullRequest, checks: source.checks, sessionID: agent.discoveredID}})
	if outcome.err != nil || !outcome.skipped || len(agent.requests) != 0 {
		t.Fatalf("outcome=%+v requests=%d", outcome, len(agent.requests))
	}
	source.openError = errors.New("temporary listing failure")
	if _, err := monitor.RunOnce(context.Background()); err == nil {
		t.Fatal("listing failure lost")
	}
	latest, _ := loadState(monitor.stateFile)
	if latest.CI["owner/repo#42"] == nil {
		t.Fatal("failed scan removed state")
	}
	source.openError = nil
	source.pullRequests = []githubapi.PullRequest{}
	if _, err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	latest, _ = loadState(monitor.stateFile)
	if len(latest.CI) != 0 {
		t.Fatal("closed PR pause retained")
	}
}

func TestCIScanFailureDoesNotBlockReviewDelivery(t *testing.T) {
	t.Parallel()
	monitor, source, _ := ciFixture(t)
	source.checksError = errors.New("CI unavailable")
	source.threads = []githubapi.ReviewThread{reviewThread("review", time.Now())}
	if r, err := monitor.RunOnce(context.Background()); err == nil || r.Dispatches != 1 {
		t.Fatalf("result=%+v err=%v", r, err)
	}
}
