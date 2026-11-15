package prmanager

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/matan-yadgar/bifrost/internal/github"
	"github.com/matan-yadgar/bifrost/internal/harness"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

type fakeSource struct {
	pullRequest github.PullRequest
}

func (source *fakeSource) PullRequest(_ context.Context, repository string, number int) (github.PullRequest, error) {
	if repository != source.pullRequest.Repository || number != source.pullRequest.Number {
		return github.PullRequest{}, context.Canceled
	}
	return source.pullRequest, nil
}

type fakeHarness struct {
	requests   []harness.Request
	dispatched chan harness.Request
}

type fakeReviewCache struct {
	heads map[string]string
}

func (cache *fakeReviewCache) PRManagerReviewHandled(_ context.Context, key, headSHA string) (bool, error) {
	return cache.heads[key] == headSHA, nil
}

func (cache *fakeReviewCache) MarkPRManagerReviewHandled(_ context.Context, key, headSHA string) error {
	cache.heads[key] = headSHA
	return nil
}

func (agentHarness *fakeHarness) Name() string { return "fake" }

func (agentHarness *fakeHarness) Discover(context.Context, []harness.Target) ([]harness.Discovery, error) {
	return nil, nil
}

func (agentHarness *fakeHarness) Dispatch(_ context.Context, request harness.Request) (harness.Result, error) {
	agentHarness.requests = append(agentHarness.requests, request)
	if agentHarness.dispatched != nil {
		agentHarness.dispatched <- request
	}
	return harness.Result{SessionID: "019f0000-0000-7000-8000-000000000001"}, nil
}

type fakeAcknowledger struct {
	envelopeIDs []string
}

func (acknowledger *fakeAcknowledger) AckCtx(_ context.Context, envelopeID string, _ any) error {
	acknowledger.envelopeIDs = append(acknowledger.envelopeIDs, envelopeID)
	return nil
}

func TestRequestsAcceptsOnlyConfiguredPRManagerReviewMessages(t *testing.T) {
	t.Parallel()
	manager := New(Settings{Conversation: "D123", BotID: "B123"}, []Repository{{Name: "Owner/Repo", WorkingDirectory: "/repo"}}, time.Minute, log.New(io.Discard, "", 0))
	accepted := manager.requests(message{
		Conversation: "D123", BotID: "B123", ChannelType: "im", Subtype: "bot_message",
		Text: "Please review: https://github.com/owner/repo/pull/42 and https://github.com/owner/repo/pull/42",
	})
	if len(accepted) != 1 || accepted[0].Repository.Name != "Owner/Repo" || accepted[0].PullRequest != 42 {
		t.Fatalf("accepted requests = %#v", accepted)
	}
	for _, event := range []message{
		{Conversation: "D123", BotID: "B999", ChannelType: "im", Subtype: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42"},
		{Conversation: "D123", BotID: "B123", ChannelType: "channel", Subtype: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42"},
		{Conversation: "D123", BotID: "B123", ChannelType: "im", Subtype: "bot_message", Text: "PR manager moved its automatic review follow-up to @danielwolfman."},
		{Conversation: "D123", BotID: "B123", ChannelType: "im", Subtype: "", Text: "Please review: https://github.com/owner/repo/pull/42"},
	} {
		if requests := manager.requests(event); len(requests) != 0 {
			t.Fatalf("untrusted event produced %#v", requests)
		}
	}
}

func TestDeliverDeduplicatesByCurrentHeadAcrossRestart(t *testing.T) {
	t.Parallel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, State: "open", URL: "https://github.com/owner/repo/pull/42", Title: "Improve Bifrost", HeadSHA: "first"}}
	agentHarness := &fakeHarness{}
	cache := &fakeReviewCache{heads: make(map[string]string)}
	manager := New(Settings{}, nil, time.Minute, log.New(io.Discard, "", 0))
	manager.reviewCache = cache
	request := reviewRequest{Repository: Repository{Name: "owner/repo", WorkingDirectory: "/repo"}, PullRequest: 42, PullRequestURL: "https://github.com/owner/repo/pull/42"}

	manager.deliver(context.Background(), request, source, agentHarness)
	manager.deliver(context.Background(), request, source, agentHarness)
	if len(agentHarness.requests) != 1 {
		t.Fatalf("dispatches after duplicate notification = %d", len(agentHarness.requests))
	}
	if agentHarness.requests[0].WorkingDirectory != "/repo" || agentHarness.requests[0].Prompt != "/trigger-code-review https://github.com/owner/repo/pull/42 and paste the comments in the PR" {
		t.Fatalf("review request = %#v", agentHarness.requests[0])
	}

	restartedManager := New(Settings{}, nil, time.Minute, log.New(io.Discard, "", 0))
	restartedManager.reviewCache = cache
	restartedManager.deliver(context.Background(), request, source, agentHarness)
	if len(agentHarness.requests) != 1 {
		t.Fatalf("dispatches after restart = %d", len(agentHarness.requests))
	}

	source.pullRequest.HeadSHA = "second"
	restartedManager.deliver(context.Background(), request, source, agentHarness)
	if len(agentHarness.requests) != 2 {
		t.Fatalf("dispatches after a new head = %d", len(agentHarness.requests))
	}
}

func TestConnectionStatusTracksReconnects(t *testing.T) {
	t.Parallel()
	manager := New(Settings{}, nil, time.Minute, log.New(io.Discard, "", 0))
	manager.running = true
	manager.generation = 1

	manager.setConnected(1)
	if status := manager.Status(); !status.Connected || status.LastError != "" {
		t.Fatalf("connected status = %#v", status)
	}
	manager.setConnecting(1)
	if status := manager.Status(); status.Connected {
		t.Fatalf("reconnecting status = %#v", status)
	}
	manager.setConnectionError(1, errors.New("network unavailable"))
	if status := manager.Status(); status.Connected || status.LastError != "network unavailable" {
		t.Fatalf("connection error status = %#v", status)
	}
	manager.setConnected(1)
	if status := manager.Status(); !status.Connected || status.LastError != "" {
		t.Fatalf("reconnected status = %#v", status)
	}
}

func TestHandleSocketEventAcknowledgesAndDispatchesTrustedReview(t *testing.T) {
	context, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, State: "open", URL: "https://github.com/owner/repo/pull/42", Title: "Improve Bifrost", HeadSHA: "reviewed"}}
	agentHarness := &fakeHarness{dispatched: make(chan harness.Request, 1)}
	manager := New(Settings{Conversation: "D123", BotID: "B123"}, []Repository{{Name: "owner/repo", WorkingDirectory: "/repo"}}, time.Minute, log.New(io.Discard, "", 0))
	manager.reviewCache = &fakeReviewCache{heads: make(map[string]string)}
	jobs := make(chan reviewRequest, 1)
	manager.mutex.Lock()
	manager.running = true
	manager.jobs = jobs
	manager.mutex.Unlock()
	go manager.runWorker(context, jobs, source, agentHarness)
	acknowledger := &fakeAcknowledger{}

	manager.handleSocketEvent(context, acknowledger, socketmode.Event{
		Type:    socketmode.EventTypeEventsAPI,
		Request: &socketmode.Request{EnvelopeID: "trusted"},
		Data: slackevents.EventsAPIEvent{Type: slackevents.CallbackEvent, InnerEvent: slackevents.EventsAPIInnerEvent{Data: &slackevents.MessageEvent{
			Channel: "D123", BotID: "B123", ChannelType: "im", SubType: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42",
		}}},
	})
	select {
	case request := <-agentHarness.dispatched:
		if request.WorkingDirectory != "/repo" || request.Prompt != "/trigger-code-review https://github.com/owner/repo/pull/42 and paste the comments in the PR" {
			t.Fatalf("dispatched review = %#v", request)
		}
	case <-time.After(time.Second):
		t.Fatal("trusted Slack review was not dispatched")
	}

	manager.handleSocketEvent(context, acknowledger, socketmode.Event{
		Type:    socketmode.EventTypeEventsAPI,
		Request: &socketmode.Request{EnvelopeID: "unconfigured"},
		Data: slackevents.EventsAPIEvent{Type: slackevents.CallbackEvent, InnerEvent: slackevents.EventsAPIInnerEvent{Data: &slackevents.MessageEvent{
			Channel: "D123", BotID: "B123", ChannelType: "im", SubType: "bot_message", Text: "Please review: https://github.com/other/repo/pull/42",
		}}},
	})
	if len(acknowledger.envelopeIDs) != 2 || acknowledger.envelopeIDs[0] != "trusted" || acknowledger.envelopeIDs[1] != "unconfigured" {
		t.Fatalf("acknowledged events = %#v", acknowledger.envelopeIDs)
	}
	select {
	case request := <-agentHarness.dispatched:
		t.Fatalf("unconfigured repository dispatched %#v", request)
	default:
	}
}

func TestDeliverSkipsClosedPullRequest(t *testing.T) {
	t.Parallel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, State: "closed"}}
	agentHarness := &fakeHarness{}
	manager := New(Settings{}, nil, time.Minute, log.New(io.Discard, "", 0))
	manager.reviewCache = &fakeReviewCache{heads: make(map[string]string)}
	manager.deliver(context.Background(), reviewRequest{Repository: Repository{Name: "owner/repo", WorkingDirectory: "/repo"}, PullRequest: 42}, source, agentHarness)
	if len(agentHarness.requests) != 0 {
		t.Fatalf("dispatches for closed pull request = %d", len(agentHarness.requests))
	}
}

func TestManagerConnectsWhenSlackTokensConfigured(t *testing.T) {
	if os.Getenv("BIFROST_LIVE_SLACK_TEST") != "1" {
		t.Skip("set BIFROST_LIVE_SLACK_TEST=1 to run against Slack")
	}
	manager := New(Settings{
		Enabled: true, AppTokenEnv: "BIFROST_SLACK_APP_TOKEN", UserTokenEnv: "BIFROST_SLACK_USER_TOKEN",
		Conversation: "Dtest", BotID: "Btest",
	}, nil, time.Minute, log.New(io.Discard, "", 0))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := manager.Start(ctx, &fakeSource{}, &fakeHarness{}, &fakeReviewCache{heads: make(map[string]string)}); err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	for deadline := time.After(10 * time.Second); ; {
		status := manager.Status()
		if status.Connected {
			return
		}
		if status.LastError != "" {
			t.Fatalf("Slack listener failed: %s", status.LastError)
		}
		select {
		case <-deadline:
			t.Fatal("Slack listener did not connect")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
