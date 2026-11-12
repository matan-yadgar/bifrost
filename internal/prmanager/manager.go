package prmanager

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matan-yadgar/bifrost/internal/github"
	"github.com/matan-yadgar/bifrost/internal/harness"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

const maxPendingReviews = 32

var pullRequestURL = regexp.MustCompile(`https://github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)/pull/([1-9][0-9]*)`)

type Settings struct {
	Enabled      bool
	AppTokenEnv  string
	UserTokenEnv string
	Conversation string
	BotID        string
}

type Repository struct {
	Name             string
	WorkingDirectory string
}

type Status struct {
	Enabled   bool   `json:"enabled"`
	Running   bool   `json:"running"`
	Connected bool   `json:"connected"`
	LastError string `json:"last_error,omitempty"`
}

type Source interface {
	PullRequest(context.Context, string, int) (github.PullRequest, error)
}

type Manager struct {
	settings        Settings
	repositories    map[string]Repository
	dispatchTimeout time.Duration
	logger          *log.Logger

	mutex      sync.Mutex
	enabled    bool
	running    bool
	connected  bool
	lastError  string
	parent     context.Context
	source     Source
	harness    harness.Harness
	cancel     context.CancelFunc
	jobs       chan reviewRequest
	generation uint64
	delivered  map[string]string
}

type message struct {
	Conversation string
	BotID        string
	ChannelType  string
	Subtype      string
	Text         string
}

type reviewRequest struct {
	Repository     Repository
	PullRequest    int
	PullRequestURL string
}

func New(settings Settings, repositories []Repository, dispatchTimeout time.Duration, logger *log.Logger) *Manager {
	configured := make(map[string]Repository, len(repositories))
	for _, repository := range repositories {
		configured[strings.ToLower(repository.Name)] = repository
	}
	return &Manager{
		settings: settings, repositories: configured, dispatchTimeout: dispatchTimeout,
		logger: logger, enabled: settings.Enabled, delivered: make(map[string]string),
	}
}

// Start makes the manager available to the local control UI. It only opens a
// Slack connection when enabled in config or by the UI.
func (manager *Manager) Start(ctx context.Context, source Source, agentHarness harness.Harness) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.parent = ctx
	manager.source = source
	manager.harness = agentHarness
	if !manager.enabled {
		return nil
	}
	return manager.startLocked()
}

func (manager *Manager) Stop() {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	manager.parent = nil
	manager.source = nil
	manager.harness = nil
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

func (manager *Manager) Status() Status {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return Status{Enabled: manager.enabled, Running: manager.running, Connected: manager.connected, LastError: manager.lastError}
}

func (manager *Manager) startLocked() error {
	if manager.running {
		return nil
	}
	if manager.parent == nil || manager.source == nil || manager.harness == nil {
		return manager.failLocked(fmt.Errorf("bifrost must be active before PR Manager can start"))
	}
	if manager.settings.Conversation == "" || manager.settings.BotID == "" {
		return manager.failLocked(fmt.Errorf("pr_manager.conversation_id and pr_manager.bot_id are required"))
	}
	appToken := strings.TrimSpace(os.Getenv(manager.settings.AppTokenEnv))
	if appToken == "" {
		return manager.failLocked(fmt.Errorf("%s is required", manager.settings.AppTokenEnv))
	}
	userToken := strings.TrimSpace(os.Getenv(manager.settings.UserTokenEnv))
	if userToken == "" {
		return manager.failLocked(fmt.Errorf("%s is required", manager.settings.UserTokenEnv))
	}

	ctx, cancel := context.WithCancel(manager.parent)
	manager.generation++
	generation := manager.generation
	jobs := make(chan reviewRequest, maxPendingReviews)
	manager.cancel = cancel
	manager.jobs = jobs
	manager.running = true
	manager.connected = false
	manager.lastError = ""
	go manager.runWorker(ctx, jobs, manager.source, manager.harness)
	go manager.runSocket(ctx, cancel, generation, appToken, userToken)
	manager.logger.Printf("PR Manager listener started: conversation=%s", manager.settings.Conversation)
	return nil
}

func (manager *Manager) failLocked(err error) error {
	manager.lastError = err.Error()
	return err
}

func (manager *Manager) stopLocked() {
	manager.generation++
	if manager.cancel != nil {
		manager.cancel()
	}
	manager.cancel = nil
	manager.jobs = nil
	manager.running = false
	manager.connected = false
}

func (manager *Manager) runSocket(ctx context.Context, cancel context.CancelFunc, generation uint64, appToken, userToken string) {
	defer cancel()
	api := slack.New(userToken, slack.OptionAppLevelToken(appToken))
	client := socketmode.New(api, socketmode.OptionReadLimit(1024*1024))
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-client.Events:
				if event.Type == socketmode.EventTypeConnected {
					manager.setConnected(generation)
					continue
				}
				manager.handleSocketEvent(ctx, client, event)
			}
		}
	}()
	err := client.RunContext(ctx)
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.generation != generation {
		return
	}
	manager.running = false
	manager.connected = false
	manager.cancel = nil
	manager.jobs = nil
	if ctx.Err() == nil {
		manager.lastError = err.Error()
		manager.logger.Printf("PR Manager listener stopped: %v", err)
		return
	}
	manager.logger.Print("PR Manager listener stopped")
}

func (manager *Manager) setConnected(generation uint64) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.generation != generation || !manager.running {
		return
	}
	manager.connected = true
	manager.logger.Print("PR Manager listener connected")
}

func (manager *Manager) handleSocketEvent(ctx context.Context, client *socketmode.Client, event socketmode.Event) {
	if event.Type != socketmode.EventTypeEventsAPI || event.Request == nil {
		return
	}
	if err := client.Ack(*event.Request); err != nil {
		manager.logger.Printf("acknowledge PR Manager Slack event: %v", err)
		return
	}
	eventsAPIEvent, ok := event.Data.(slackevents.EventsAPIEvent)
	if !ok || eventsAPIEvent.Type != slackevents.CallbackEvent {
		return
	}
	slackMessage, ok := eventsAPIEvent.InnerEvent.Data.(*slackevents.MessageEvent)
	if !ok {
		return
	}
	manager.enqueue(message{
		Conversation: slackMessage.Channel,
		BotID:        slackMessage.BotID,
		ChannelType:  slackMessage.ChannelType,
		Subtype:      slackMessage.SubType,
		Text:         slackMessage.Text,
	})
}

func (manager *Manager) enqueue(event message) {
	requests := manager.requests(event)
	if len(requests) == 0 {
		return
	}
	manager.mutex.Lock()
	jobs := manager.jobs
	running := manager.running
	manager.mutex.Unlock()
	if !running || jobs == nil {
		return
	}
	for _, request := range requests {
		select {
		case jobs <- request:
			manager.logger.Printf("PR Manager review queued: pr=%s#%d", request.Repository.Name, request.PullRequest)
		default:
			manager.logger.Printf("PR Manager review ignored: queue is full pr=%s#%d", request.Repository.Name, request.PullRequest)
		}
	}
}

func (manager *Manager) requests(event message) []reviewRequest {
	if event.Conversation != manager.settings.Conversation || event.BotID != manager.settings.BotID || event.ChannelType != "im" || event.Subtype != "bot_message" || !strings.HasPrefix(strings.TrimSpace(event.Text), "Please review:") {
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
		requests = append(requests, reviewRequest{Repository: repository, PullRequest: number, PullRequestURL: match[0]})
	}
	return requests
}

func (manager *Manager) runWorker(ctx context.Context, jobs <-chan reviewRequest, source Source, agentHarness harness.Harness) {
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-jobs:
			manager.deliver(ctx, request, source, agentHarness)
		}
	}
}

func (manager *Manager) deliver(ctx context.Context, request reviewRequest, source Source, agentHarness harness.Harness) {
	pullRequest, err := source.PullRequest(ctx, request.Repository.Name, request.PullRequest)
	if err != nil {
		manager.logger.Printf("PR Manager review failed: pr=%s#%d reason=get pull request: %v", request.Repository.Name, request.PullRequest, err)
		return
	}
	key := strings.ToLower(pullRequest.Repository) + "#" + strconv.Itoa(pullRequest.Number)
	manager.mutex.Lock()
	if manager.delivered[key] == pullRequest.HeadSHA {
		manager.mutex.Unlock()
		manager.logger.Printf("PR Manager review ignored: pr=%s unchanged", key)
		return
	}
	manager.mutex.Unlock()

	dispatchContext, cancel := context.WithTimeout(ctx, manager.dispatchTimeout)
	defer cancel()
	manager.logger.Printf("PR Manager review started: pr=%s", key)
	_, err = agentHarness.Dispatch(dispatchContext, harness.Request{
		WorkingDirectory: request.Repository.WorkingDirectory,
		Prompt:           reviewPrompt(pullRequest, request.PullRequestURL),
	})
	if err != nil {
		manager.logger.Printf("PR Manager review failed: pr=%s reason=%v", key, err)
		return
	}
	manager.mutex.Lock()
	manager.delivered[key] = pullRequest.HeadSHA
	manager.mutex.Unlock()
	manager.logger.Printf("PR Manager review task started: pr=%s", key)
}

func reviewPrompt(pullRequest github.PullRequest, pullRequestURL string) string {
	return fmt.Sprintf(`PR Manager requested a read-only review of %s.

Inspect the current pull request, its diff, and relevant repository context. Identify concrete concerns and propose the smallest appropriate changes, if any.

Do not modify files, commit, push, post a GitHub review/comment, or otherwise mutate local or remote state. Write a concise review in this Codex task and then finish.

PR: %s
Title: %s
`, pullRequestURL, pullRequest.URL, pullRequest.Title)
}
