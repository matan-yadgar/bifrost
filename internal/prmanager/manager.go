package prmanager

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matan-yadgar/bifrost/internal/github"
	"github.com/matan-yadgar/bifrost/internal/harness"
	"github.com/slack-go/slack"
)

const (
	slackHistoryPageSize = 100
	slackRequestTimeout  = 30 * time.Second
)

var pullRequestURL = regexp.MustCompile(`https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/pull/([1-9][0-9]*)`)

type Settings struct {
	Enabled        bool
	AutoReply      bool
	UserTokenEnv   string
	Conversation   string
	BotID          string
	TemplateTaskID string
}

type Repository struct {
	Name             string
	WorkingDirectory string
}

type Status struct {
	Enabled   bool   `json:"enabled"`
	Running   bool   `json:"running"`
	LastError string `json:"last_error,omitempty"`
}

type Source interface {
	PullRequest(context.Context, string, int) (github.PullRequest, error)
}

type ReviewCache interface {
	PRManagerCursor(context.Context) (string, error)
	SetPRManagerCursor(context.Context, string) error
	PRManagerReviewHandled(context.Context, string, string) (bool, error)
	MarkPRManagerReviewHandled(context.Context, string, string) error
}

type messageSource interface {
	Messages(context.Context, string, string) ([]message, error)
}

type slackMessageSource struct {
	client *slack.Client
}

type Manager struct {
	settings        Settings
	repositories    map[string]Repository
	dispatchTimeout time.Duration
	logger          *log.Logger

	mutex         sync.Mutex
	enabled       bool
	running       bool
	lastError     string
	source        Source
	harness       harness.Harness
	reviewCache   ReviewCache
	messageSource messageSource
}

type message struct {
	Conversation string
	BotID        string
	Subtype      string
	Text         string
	Timestamp    string
}

type reviewRequest struct {
	Repository     Repository
	PullRequest    int
	PullRequestURL string
	AutoReply      bool
	TemplateTaskID string
}

func New(settings Settings, repositories []Repository, dispatchTimeout time.Duration, logger *log.Logger) *Manager {
	configured := make(map[string]Repository, len(repositories))
	for _, repository := range repositories {
		configured[strings.ToLower(repository.Name)] = repository
	}
	return &Manager{
		settings: settings, repositories: configured, dispatchTimeout: dispatchTimeout,
		logger: logger, enabled: settings.Enabled,
	}
}

// Start makes the manager available to the local control UI. Slack history is
// read by Poll, alongside Bifrost's normal GitHub polling cycle.
func (manager *Manager) Start(_ context.Context, source Source, agentHarness harness.Harness, reviewCache ReviewCache) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.source = source
	manager.harness = agentHarness
	manager.reviewCache = reviewCache
	if !manager.enabled {
		return nil
	}
	return manager.startLocked()
}

func (manager *Manager) Stop() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.source = nil
	manager.harness = nil
	manager.reviewCache = nil
	manager.stopLocked()
}

func (manager *Manager) Activate() error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.enabled = true
	return manager.startLocked()
}

func (manager *Manager) Deactivate() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.enabled = false
	manager.lastError = ""
	manager.stopLocked()
}

// SetAutoReply changes the reply mode for reviews started after this call.
func (manager *Manager) SetAutoReply(autoReply bool) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.settings.AutoReply = autoReply
}

func (manager *Manager) Status() Status {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return Status{Enabled: manager.enabled, Running: manager.running, LastError: manager.lastError}
}

// Poll processes new trusted PR Manager messages. The first poll establishes a
// cursor and deliberately leaves existing history alone, matching Socket Mode's
// former future-events-only behavior.
func (manager *Manager) Poll(ctx context.Context) error {
	settings, source, agentHarness, reviewCache, messages, running := manager.pollDependencies()
	if !running {
		return nil
	}
	cursor, err := reviewCache.PRManagerCursor(ctx)
	if err != nil {
		return manager.pollFailed(fmt.Errorf("read PR Manager cursor: %w", err))
	}
	events, err := messages.Messages(ctx, settings.Conversation, cursor)
	if err != nil {
		return manager.pollFailed(fmt.Errorf("read PR Manager Slack history: %w", err))
	}
	sort.Slice(events, func(left, right int) bool { return events[left].Timestamp < events[right].Timestamp })
	if cursor == "" {
		cursor = newestTimestamp(events)
		if cursor == "" {
			cursor = slackTimestamp(time.Now())
		}
		if err := reviewCache.SetPRManagerCursor(ctx, cursor); err != nil {
			return manager.pollFailed(fmt.Errorf("save PR Manager cursor: %w", err))
		}
		manager.pollSucceeded()
		manager.logger.Print("PR Manager history baseline established")
		return nil
	}

	nextCursor := cursor
	for _, event := range events {
		if event.Timestamp <= cursor {
			continue
		}
		for _, request := range manager.requests(event, settings) {
			request.AutoReply = settings.AutoReply
			if err := manager.deliver(ctx, request, source, agentHarness, reviewCache); err != nil {
				return manager.pollFailed(err)
			}
		}
		if event.Timestamp > nextCursor {
			nextCursor = event.Timestamp
		}
	}
	if nextCursor != cursor {
		if err := reviewCache.SetPRManagerCursor(ctx, nextCursor); err != nil {
			return manager.pollFailed(fmt.Errorf("save PR Manager cursor: %w", err))
		}
	}
	manager.pollSucceeded()
	return nil
}

func (manager *Manager) startLocked() error {
	if manager.running {
		return nil
	}
	if manager.source == nil || manager.harness == nil {
		return manager.failLocked(fmt.Errorf("bifrost must be active before PR Manager can start"))
	}
	if manager.reviewCache == nil {
		return manager.failLocked(fmt.Errorf("pr manager review cache is required"))
	}
	if manager.settings.Conversation == "" || manager.settings.BotID == "" || manager.settings.TemplateTaskID == "" {
		return manager.failLocked(fmt.Errorf("pr_manager.conversation_id, pr_manager.bot_id, and pr_manager.template_task_id are required"))
	}
	userToken := strings.TrimSpace(os.Getenv(manager.settings.UserTokenEnv))
	if userToken == "" {
		return manager.failLocked(fmt.Errorf("%s is required", manager.settings.UserTokenEnv))
	}
	manager.messageSource = &slackMessageSource{client: slack.New(userToken, slack.OptionHTTPClient(&http.Client{Timeout: slackRequestTimeout}))}
	manager.running = true
	manager.lastError = ""
	manager.logger.Printf("PR Manager polling enabled: conversation=%s", manager.settings.Conversation)
	return nil
}

func (manager *Manager) failLocked(err error) error {
	manager.lastError = err.Error()
	return err
}

func (manager *Manager) stopLocked() {
	manager.messageSource = nil
	manager.running = false
}

func (manager *Manager) pollDependencies() (Settings, Source, harness.Harness, ReviewCache, messageSource, bool) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return manager.settings, manager.source, manager.harness, manager.reviewCache, manager.messageSource, manager.enabled && manager.running
}

func (manager *Manager) pollFailed(err error) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.running {
		manager.lastError = err.Error()
	}
	return err
}

func (manager *Manager) pollSucceeded() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.running {
		manager.lastError = ""
	}
}

func (manager *Manager) requests(event message, settings Settings) []reviewRequest {
	if event.Conversation != settings.Conversation || event.BotID != settings.BotID || (event.Subtype != "" && event.Subtype != "bot_message") || !strings.HasPrefix(strings.TrimSpace(event.Text), "Please review:") {
		return nil
	}
	seen := make(map[string]bool)
	var requests []reviewRequest
	for _, match := range pullRequestURL.FindAllStringSubmatch(event.Text, -1) {
		repositoryName := match[1] + "/" + match[2]
		repository, found := manager.repositories[strings.ToLower(repositoryName)]
		if !found {
			continue
		}
		number, _ := strconv.Atoi(match[3])
		key := strings.ToLower(repository.Name) + "#" + strconv.Itoa(number)
		if seen[key] {
			continue
		}
		seen[key] = true
		requests = append(requests, reviewRequest{Repository: repository, PullRequest: number, PullRequestURL: match[0], TemplateTaskID: settings.TemplateTaskID})
	}
	return requests
}

func (manager *Manager) deliver(ctx context.Context, request reviewRequest, source Source, agentHarness harness.Harness, reviewCache ReviewCache) error {
	pullRequest, err := source.PullRequest(ctx, request.Repository.Name, request.PullRequest)
	if err != nil {
		return fmt.Errorf("PR Manager review failed: pr=%s#%d reason=get pull request: %w", request.Repository.Name, request.PullRequest, err)
	}
	if pullRequest.State != "open" {
		manager.logger.Printf("PR Manager review ignored: pr=%s#%d is no longer open", request.Repository.Name, request.PullRequest)
		return nil
	}
	key := strings.ToLower(pullRequest.Repository) + "#" + strconv.Itoa(pullRequest.Number)
	handled, err := reviewCache.PRManagerReviewHandled(ctx, key, pullRequest.HeadSHA)
	if err != nil {
		return fmt.Errorf("PR Manager review failed: pr=%s reason=read review cache: %w", key, err)
	}
	if handled {
		manager.logger.Printf("PR Manager review ignored: pr=%s unchanged", key)
		return nil
	}

	dispatchContext, cancel := context.WithTimeout(ctx, manager.dispatchTimeout)
	defer cancel()
	manager.logger.Printf("PR Manager review started: pr=%s", key)
	discoveries, err := agentHarness.Discover(dispatchContext, []harness.Target{{
		Repository: pullRequest.Repository, PullRequest: pullRequest.Number,
		CreatorSessionID: request.TemplateTaskID, WorkingDirectory: request.Repository.WorkingDirectory,
	}})
	if err != nil {
		return fmt.Errorf("PR Manager review failed: pr=%s reason=fork template task: %w", key, err)
	}
	if len(discoveries) != 1 || discoveries[0].Err != nil || !discoveries[0].Found || discoveries[0].Session.ID == "" {
		if len(discoveries) == 1 && discoveries[0].Err != nil {
			return fmt.Errorf("PR Manager review failed: pr=%s reason=fork template task: %w", key, discoveries[0].Err)
		}
		return fmt.Errorf("PR Manager review failed: pr=%s reason=fork template task did not return a session", key)
	}
	_, err = agentHarness.Dispatch(dispatchContext, harness.Request{
		SessionID: discoveries[0].Session.ID, WorkingDirectory: request.Repository.WorkingDirectory,
		Prompt: reviewPrompt(request.PullRequestURL, pullRequest.HeadSHA, request.AutoReply),
	})
	if err != nil {
		return fmt.Errorf("PR Manager review failed: pr=%s reason=dispatch: %w", key, err)
	}
	if err := reviewCache.MarkPRManagerReviewHandled(ctx, key, pullRequest.HeadSHA); err != nil {
		return fmt.Errorf("PR Manager review failed: pr=%s reason=save review cache: %w", key, err)
	}
	manager.logger.Printf("PR Manager review task started: pr=%s", key)
	return nil
}

func (source *slackMessageSource) Messages(ctx context.Context, conversation, oldest string) ([]message, error) {
	response, err := source.client.GetConversationHistoryContext(ctx, &slack.GetConversationHistoryParameters{
		ChannelID: conversation,
		Oldest:    oldest,
		Limit:     slackHistoryPageSize,
	})
	if err != nil {
		return nil, err
	}
	events := make([]message, 0, len(response.Messages))
	for _, slackMessage := range response.Messages {
		if slackMessage.Timestamp == "" {
			continue
		}
		events = append(events, message{
			Conversation: conversation,
			BotID:        slackMessage.BotID,
			Subtype:      slackMessage.SubType,
			Text:         slackMessage.Text,
			Timestamp:    slackMessage.Timestamp,
		})
	}
	return events, nil
}

func newestTimestamp(events []message) string {
	var newest string
	for _, event := range events {
		if event.Timestamp > newest {
			newest = event.Timestamp
		}
	}
	return newest
}

func slackTimestamp(now time.Time) string {
	return fmt.Sprintf("%d.%06d", now.Unix(), now.Nanosecond()/int(time.Microsecond))
}

func reviewPrompt(pullRequestURL, headSHA string, autoReply bool) string {
	if autoReply {
		return fmt.Sprintf("/trigger-code-review %s Review exactly commit %s. Post only actionable review comments. If all actionable comments are P3, paste them in the PR and approve it if GitHub permits it. If any actionable comment is P1 or P2, paste the comments but do not approve. If there are no actionable comments, do not post a comment; approve the PR if GitHub permits it, otherwise stop without making any write.", pullRequestURL, headSHA)
	}
	return fmt.Sprintf("/trigger-code-review %s Review exactly commit %s. Do not post review comments; present the findings in this Codex task and wait for user approval.", pullRequestURL, headSHA)
}
