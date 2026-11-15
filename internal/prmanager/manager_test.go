package prmanager

import (
	"context"
	"errors"
	"io"
	"log"
	"testing"
	"time"

	"github.com/matan-yadgar/bifrost/internal/github"
	"github.com/matan-yadgar/bifrost/internal/harness"
)

type fakeSource struct {
	pullRequest github.PullRequest
	err         error
}

func (source *fakeSource) PullRequest(_ context.Context, repository string, number int) (github.PullRequest, error) {
	if source.err != nil {
		return github.PullRequest{}, source.err
	}
	if repository != source.pullRequest.Repository || number != source.pullRequest.Number {
		return github.PullRequest{}, context.Canceled
	}
	return source.pullRequest, nil
}

type fakeHarness struct {
	requests []harness.Request
	err      error
}

func (agentHarness *fakeHarness) Name() string { return "fake" }

func (agentHarness *fakeHarness) Discover(context.Context, []harness.Target) ([]harness.Discovery, error) {
	return nil, nil
}

func (agentHarness *fakeHarness) Dispatch(_ context.Context, request harness.Request) (harness.Result, error) {
	if agentHarness.err != nil {
		return harness.Result{}, agentHarness.err
	}
	agentHarness.requests = append(agentHarness.requests, request)
	return harness.Result{SessionID: "019f0000-0000-7000-8000-000000000001"}, nil
}

type fakeReviewCache struct {
	heads     map[string]string
	cursor    string
	cursorErr error
}

func (cache *fakeReviewCache) PRManagerCursor(context.Context) (string, error) {
	return cache.cursor, cache.cursorErr
}

func (cache *fakeReviewCache) SetPRManagerCursor(_ context.Context, cursor string) error {
	if cache.cursorErr != nil {
		return cache.cursorErr
	}
	cache.cursor = cursor
	return nil
}

func (cache *fakeReviewCache) PRManagerReviewHandled(_ context.Context, key, headSHA string) (bool, error) {
	return cache.heads[key] == headSHA, nil
}

func (cache *fakeReviewCache) MarkPRManagerReviewHandled(_ context.Context, key, headSHA string) error {
	cache.heads[key] = headSHA
	return nil
}

type fakeMessageSource struct {
	messages []message
	err      error
	oldest   []string
}

func (source *fakeMessageSource) Messages(_ context.Context, _ string, oldest string) ([]message, error) {
	source.oldest = append(source.oldest, oldest)
	if source.err != nil {
		return nil, source.err
	}
	return append([]message(nil), source.messages...), nil
}

func activeManager(source Source, agentHarness harness.Harness, reviewCache ReviewCache, messages messageSource) *Manager {
	manager := New(Settings{Enabled: true, Conversation: "D123", BotID: "B123"}, []Repository{{Name: "owner/repo", WorkingDirectory: "/repo"}}, time.Minute, log.New(io.Discard, "", 0))
	manager.source = source
	manager.harness = agentHarness
	manager.reviewCache = reviewCache
	manager.messageSource = messages
	manager.running = true
	return manager
}

func TestRequestsAcceptsOnlyConfiguredPRManagerReviewMessages(t *testing.T) {
	t.Parallel()
	manager := New(Settings{Conversation: "D123", BotID: "B123"}, []Repository{{Name: "Owner/Repo", WorkingDirectory: "/repo"}}, time.Minute, log.New(io.Discard, "", 0))
	accepted := manager.requests(message{
		Conversation: "D123", BotID: "B123", Subtype: "bot_message",
		Text: "Please review: https://github.com/owner/repo/pull/42 and https://github.com/owner/repo/pull/42",
	}, manager.settings)
	if len(accepted) != 1 || accepted[0].Repository.Name != "Owner/Repo" || accepted[0].PullRequest != 42 {
		t.Fatalf("accepted requests = %#v", accepted)
	}
	accepted = manager.requests(message{
		Conversation: "D123", BotID: "B123",
		Text: "Please review: https://github.com/owner/repo/pull/42",
	}, manager.settings)
	if len(accepted) != 1 {
		t.Fatalf("empty-subtype requests = %#v", accepted)
	}
	for _, event := range []message{
		{Conversation: "D123", BotID: "B999", Subtype: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42"},
		{Conversation: "D123", BotID: "B123", Subtype: "bot_message", Text: "Please review: https://github.com/other/repo/pull/42"},
		{Conversation: "D123", BotID: "B123", Subtype: "bot_message", Text: "PR manager moved its automatic review follow-up to @danielwolfman."},
		{Conversation: "D123", BotID: "B123", Subtype: "message_changed", Text: "Please review: https://github.com/owner/repo/pull/42"},
	} {
		if requests := manager.requests(event, manager.settings); len(requests) != 0 {
			t.Fatalf("untrusted event produced %#v", requests)
		}
	}
}

func TestPollBaselinesHistoryThenDeliversNewTrustedReview(t *testing.T) {
	t.Parallel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, State: "open", URL: "https://github.com/owner/repo/pull/42", HeadSHA: "first"}}
	agentHarness := &fakeHarness{}
	cache := &fakeReviewCache{heads: make(map[string]string)}
	messages := &fakeMessageSource{messages: []message{{Conversation: "D123", BotID: "B123", Subtype: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42", Timestamp: "1.000001"}}}
	manager := activeManager(source, agentHarness, cache, messages)

	if err := manager.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 0 || cache.cursor != "1.000001" {
		t.Fatalf("baseline dispatches=%d cursor=%q", len(agentHarness.requests), cache.cursor)
	}

	messages.messages = append(messages.messages, message{Conversation: "D123", BotID: "B123", Subtype: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42", Timestamp: "2.000001"})
	if err := manager.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 1 {
		t.Fatalf("dispatches = %d", len(agentHarness.requests))
	}
	if request := agentHarness.requests[0]; request.WorkingDirectory != "/repo" || request.Prompt != "/trigger-code-review https://github.com/owner/repo/pull/42 Review exactly commit first. Do not post review comments; present the findings in this Codex task and wait for user approval." {
		t.Fatalf("review request = %#v", request)
	}
	if cache.cursor != "2.000001" || cache.heads["owner/repo#42"] != "first" {
		t.Fatalf("cache = %#v", cache)
	}
}

func TestSetAutoReplyAppliesToNewPolledReview(t *testing.T) {
	t.Parallel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, State: "open", HeadSHA: "first"}}
	agentHarness := &fakeHarness{}
	cache := &fakeReviewCache{heads: make(map[string]string), cursor: "1.000001"}
	messages := &fakeMessageSource{messages: []message{{Conversation: "D123", BotID: "B123", Subtype: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42", Timestamp: "2.000001"}}}
	manager := activeManager(source, agentHarness, cache, messages)
	manager.SetAutoReply(true)

	if err := manager.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 1 || agentHarness.requests[0].Prompt != "/trigger-code-review https://github.com/owner/repo/pull/42 Review exactly commit first and paste the comments in the PR." {
		t.Fatalf("review requests = %#v", agentHarness.requests)
	}
}

func TestPollIgnoresUntrustedMessagesAndAdvancesCursor(t *testing.T) {
	t.Parallel()
	cache := &fakeReviewCache{heads: make(map[string]string), cursor: "1.000001"}
	messages := &fakeMessageSource{messages: []message{{Conversation: "D123", BotID: "B999", Subtype: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42", Timestamp: "2.000001"}}}
	agentHarness := &fakeHarness{}
	manager := activeManager(&fakeSource{}, agentHarness, cache, messages)

	if err := manager.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cache.cursor != "2.000001" || len(agentHarness.requests) != 0 {
		t.Fatalf("cursor=%q dispatches=%d", cache.cursor, len(agentHarness.requests))
	}
}

func TestPollRetriesDispatchFailureWithoutAdvancingCursor(t *testing.T) {
	t.Parallel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, State: "open", HeadSHA: "first"}}
	cache := &fakeReviewCache{heads: make(map[string]string), cursor: "1.000001"}
	messages := &fakeMessageSource{messages: []message{{Conversation: "D123", BotID: "B123", Subtype: "bot_message", Text: "Please review: https://github.com/owner/repo/pull/42", Timestamp: "2.000001"}}}
	manager := activeManager(source, &fakeHarness{err: errors.New("Codex unavailable")}, cache, messages)

	err := manager.Poll(context.Background())
	if err == nil || cache.cursor != "1.000001" || manager.Status().LastError == "" {
		t.Fatalf("err=%v cursor=%q status=%#v", err, cache.cursor, manager.Status())
	}
}

func TestDeliverDeduplicatesByCurrentHeadAcrossRestart(t *testing.T) {
	t.Parallel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, State: "open", URL: "https://github.com/owner/repo/pull/42", HeadSHA: "first"}}
	agentHarness := &fakeHarness{}
	cache := &fakeReviewCache{heads: make(map[string]string)}
	manager := New(Settings{}, nil, time.Minute, log.New(io.Discard, "", 0))
	request := reviewRequest{Repository: Repository{Name: "owner/repo", WorkingDirectory: "/repo"}, PullRequest: 42, PullRequestURL: "https://github.com/owner/repo/pull/42"}

	if err := manager.deliver(context.Background(), request, source, agentHarness, cache); err != nil {
		t.Fatal(err)
	}
	if err := manager.deliver(context.Background(), request, source, agentHarness, cache); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 1 {
		t.Fatalf("dispatches after duplicate notification = %d", len(agentHarness.requests))
	}

	restartedManager := New(Settings{}, nil, time.Minute, log.New(io.Discard, "", 0))
	if err := restartedManager.deliver(context.Background(), request, source, agentHarness, cache); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 1 {
		t.Fatalf("dispatches after restart = %d", len(agentHarness.requests))
	}

	source.pullRequest.HeadSHA = "second"
	if err := restartedManager.deliver(context.Background(), request, source, agentHarness, cache); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 2 {
		t.Fatalf("dispatches after a new head = %d", len(agentHarness.requests))
	}
}

func TestDeliverSkipsClosedPullRequest(t *testing.T) {
	t.Parallel()
	source := &fakeSource{pullRequest: github.PullRequest{Repository: "owner/repo", Number: 42, State: "closed"}}
	agentHarness := &fakeHarness{}
	manager := New(Settings{}, nil, time.Minute, log.New(io.Discard, "", 0))
	if err := manager.deliver(context.Background(), reviewRequest{Repository: Repository{Name: "owner/repo", WorkingDirectory: "/repo"}, PullRequest: 42}, source, agentHarness, &fakeReviewCache{heads: make(map[string]string)}); err != nil {
		t.Fatal(err)
	}
	if len(agentHarness.requests) != 0 {
		t.Fatalf("dispatches for closed pull request = %d", len(agentHarness.requests))
	}
}

func TestReviewPromptAutoReply(t *testing.T) {
	t.Parallel()
	prompt := reviewPrompt("https://github.com/owner/repo/pull/42", "first", true)
	if prompt != "/trigger-code-review https://github.com/owner/repo/pull/42 Review exactly commit first and paste the comments in the PR." {
		t.Fatalf("auto-reply prompt = %q", prompt)
	}
}
