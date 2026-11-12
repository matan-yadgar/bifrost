package prmanager

import (
	"context"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matan-yadgar/bifrost/internal/github"
	"github.com/matan-yadgar/bifrost/internal/harness"
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
	requests []harness.Request
}

func (agentHarness *fakeHarness) Name() string { return "fake" }

func (agentHarness *fakeHarness) Discover(context.Context, []harness.Target) ([]harness.Discovery, error) {
	return nil, nil
}

func (agentHarness *fakeHarness) Dispatch(_ context.Context, request harness.Request) (harness.Result, error) {
	agentHarness.requests = append(agentHarness.requests, request)
	return harness.Result{SessionID: "019f0000-0000-7000-8000-000000000001"}, nil
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

func TestDeliverDeduplicatesByCurrentHead(t *testing.T) {
	t.Parallel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, URL: "https://github.com/owner/repo/pull/42", Title: "Improve Bifrost", HeadSHA: "first"}}
	agentHarness := &fakeHarness{}
	manager := New(Settings{}, nil, time.Minute, log.New(io.Discard, "", 0))
	request := reviewRequest{Repository: Repository{Name: "owner/repo", WorkingDirectory: "/repo"}, PullRequest: 42, PullRequestURL: "https://github.com/owner/repo/pull/42"}

	manager.deliver(context.Background(), request, source, agentHarness)
	manager.deliver(context.Background(), request, source, agentHarness)
	if len(agentHarness.requests) != 1 {
		t.Fatalf("dispatches after duplicate notification = %d", len(agentHarness.requests))
	}
	if agentHarness.requests[0].WorkingDirectory != "/repo" || !strings.Contains(agentHarness.requests[0].Prompt, "Do not modify files") {
		t.Fatalf("review request = %#v", agentHarness.requests[0])
	}

	source.pullRequest.HeadSHA = "second"
	manager.deliver(context.Background(), request, source, agentHarness)
	if len(agentHarness.requests) != 2 {
		t.Fatalf("dispatches after a new head = %d", len(agentHarness.requests))
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
	if err := manager.Start(ctx, &fakeSource{}, &fakeHarness{}); err != nil {
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
