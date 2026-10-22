package ci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/matan-yadgar/bifrost/internal/config"
	githubapi "github.com/matan-yadgar/bifrost/internal/github"
)

type Source interface {
	OpenPullRequests(context.Context, string) ([]githubapi.PullRequest, error)
	FailedChecks(context.Context, githubapi.PullRequest) ([]githubapi.FailedCheck, error)
}

type runtime interface {
	Launch(context.Context, *batch) (string, error)
	ReadTask(context.Context, string) (json.RawMessage, error)
	Archive(context.Context, string) error
}

type Monitor struct {
	source       Source
	runtime      runtime
	repositories []config.Repository
	statePath    string
	logger       *log.Logger
	timeout      time.Duration
}

type decision struct {
	FailureID string `json:"failure_id"`
	TaskID    string `json:"task_id"`
	Outcome   string `json:"outcome"`
	Reason    string `json:"reason"`
	Revision  string `json:"revision,omitempty"`
}

type pullRequestState struct {
	HeadSHA   string              `json:"head_sha"`
	TaskName  string              `json:"task_name"`
	Decisions map[string]decision `json:"decisions"`
	Hold      *decision           `json:"hold,omitempty"`
}

type failure struct {
	ID       string                  `json:"id"`
	PRKey    string                  `json:"pr_key"`
	PRURL    string                  `json:"pr_url"`
	HeadSHA  string                  `json:"head_sha"`
	TaskName string                  `json:"task_name"`
	Checks   []githubapi.FailedCheck `json:"checks"`
}

type batch struct {
	Directory string    `json:"directory"`
	Deadline  time.Time `json:"deadline"`
	SessionID string    `json:"session_id,omitempty"`
	Failures  []failure `json:"failures"`
	Error     string    `json:"error,omitempty"`
	Applied   bool      `json:"applied,omitempty"`
}

type state struct {
	Version int                          `json:"version"`
	PRs     map[string]*pullRequestState `json:"prs"`
	Pending *batch                       `json:"pending,omitempty"`
}

func New(source Source, settings config.Runtime, environment []string, executable, configPath string, logger *log.Logger) *Monitor {
	return &Monitor{source: source, runtime: newCodex(settings, environment, executable, configPath), repositories: settings.Repositories, statePath: settings.StateFile + ".ci.json", logger: logger, timeout: settings.DispatchTimeout}
}

func (monitor *Monitor) RunOnce(ctx context.Context) error {
	memory, err := monitor.load()
	if err != nil {
		return err
	}
	var candidates []failure
	var scanErrors []error
	for _, repository := range monitor.repositories {
		pullRequests, err := monitor.source.OpenPullRequests(ctx, repository.Name)
		if err != nil {
			scanErrors = append(scanErrors, err)
			continue
		}
		open := make(map[string]bool)
		for _, pullRequest := range pullRequests {
			open[prKey(pullRequest)] = true
		}
		prefix := strings.ToLower(repository.Name) + "#"
		for key := range memory.PRs {
			if strings.HasPrefix(key, prefix) && !open[key] {
				delete(memory.PRs, key)
				monitor.logger.Printf("CI state removed: pr=%s", key)
			}
		}
		for _, pullRequest := range pullRequests {
			if !matchesAuthor(pullRequest.Author, repository.Authors) {
				continue
			}
			key := prKey(pullRequest)
			saved := memory.PRs[key]
			if saved == nil {
				saved = &pullRequestState{Decisions: make(map[string]decision)}
				memory.PRs[key] = saved
			}
			if saved.HeadSHA != pullRequest.HeadSHA {
				saved.HeadSHA = pullRequest.HeadSHA
				saved.Decisions = make(map[string]decision)
			}
			if name := pullRequest.TaskName(); name != saved.TaskName {
				saved.TaskName = name
				saved.Decisions = make(map[string]decision)
			}
			checks, err := monitor.source.FailedChecks(ctx, pullRequest)
			if err != nil {
				scanErrors = append(scanErrors, err)
				continue
			}
			if len(checks) == 0 {
				saved.Decisions = make(map[string]decision)
				continue
			}
			var unhandled []githubapi.FailedCheck
			for _, check := range checks {
				previous, handled := saved.Decisions[check.ID]
				if handled && previous.Outcome == "wait" {
					revision, readErr := monitor.taskRevision(ctx, previous.TaskID)
					if readErr != nil {
						scanErrors = append(scanErrors, readErr)
						continue
					}
					handled = revision == previous.Revision
				}
				if !handled {
					unhandled = append(unhandled, check)
				}
			}
			if len(unhandled) > 0 {
				identity := key + ":" + failureIdentity(pullRequest.HeadSHA, unhandled)
				candidates = append(candidates, failure{ID: identity, PRKey: key, PRURL: pullRequest.URL, HeadSHA: pullRequest.HeadSHA, TaskName: saved.TaskName, Checks: unhandled})
			}
		}
	}
	if err := monitor.save(memory); err != nil {
		return err
	}
	if memory.Pending != nil {
		if err := monitor.finishBatch(ctx, memory); err != nil {
			return errors.Join(errors.Join(scanErrors...), err)
		}
		if memory.Pending != nil {
			return errors.Join(scanErrors...)
		}
		// Assess candidates on the next poll after applying a batch's decisions.
		return errors.Join(scanErrors...)
	}
	selected := candidates[:0]
	for _, candidate := range candidates {
		held := false
		for _, saved := range memory.PRs {
			if saved.Hold != nil && (saved.TaskName == candidate.TaskName && candidate.TaskName != "" || saved == memory.PRs[candidate.PRKey]) {
				held = true
				break
			}
		}
		if !held && len(selected) < 25 {
			selected = append(selected, candidate)
		}
	}
	if len(selected) == 0 {
		return errors.Join(scanErrors...)
	}
	directory, err := os.MkdirTemp(filepath.Dir(monitor.statePath), "ci-batch-")
	if err != nil {
		return err
	}
	pending := &batch{Directory: directory, Failures: selected, Deadline: time.Now().Add(monitor.timeout)}
	memory.Pending = pending
	if err := writeJSON(filepath.Join(directory, "batch.json"), selected); err != nil {
		return err
	}
	if err := monitor.save(memory); err != nil {
		return err
	}
	sessionID, err := monitor.runtime.Launch(ctx, pending)
	pending.SessionID = sessionID
	if err != nil {
		pending.Error = err.Error()
	}
	if saveErr := monitor.save(memory); saveErr != nil {
		return errors.Join(err, saveErr)
	}
	if err != nil {
		return errors.Join(errors.Join(scanErrors...), err)
	}
	monitor.logger.Printf("CI coordinator started: task=%s prs=%d", sessionID, len(selected))
	return errors.Join(scanErrors...)
}

func (monitor *Monitor) finishBatch(ctx context.Context, memory *state) error {
	pending := memory.Pending
	current := pending.Failures[:0]
	for _, entry := range pending.Failures {
		if saved := memory.PRs[entry.PRKey]; saved != nil && saved.HeadSHA == entry.HeadSHA {
			current = append(current, entry)
		}
	}
	pending.Failures = current
	if err := writeJSON(filepath.Join(pending.Directory, "batch.json"), current); err != nil {
		return err
	}
	if err := monitor.save(memory); err != nil {
		return err
	}
	if len(current) == 0 {
		pending.Applied = true
		pending.Error = ""
	}
	if pending.Error != "" {
		return fmt.Errorf("CI coordinator needs attention (%s): %s; use -retry-ci after investigation", pending.Directory, pending.Error)
	}
	if !pending.Applied {
		var report struct {
			Decisions []decision `json:"decisions"`
		}
		data, err := os.ReadFile(filepath.Join(pending.Directory, "result.json"))
		if errors.Is(err, os.ErrNotExist) {
			if pending.SessionID == "" {
				return fmt.Errorf("CI batch was not launched; inspect %s and use -retry-ci", pending.Directory)
			}
			if time.Now().Before(pending.Deadline) {
				return monitor.save(memory)
			}
			pending.Error = "coordinator ended or exceeded dispatch_timeout without a result"
			if err := monitor.save(memory); err != nil {
				return err
			}
			return fmt.Errorf("CI coordinator %s ended without a result; inspect %s and use -retry-ci", pending.SessionID, pending.Directory)
		}
		if err != nil {
			return err
		}
		if err = json.Unmarshal(data, &report); err != nil {
			return monitor.failBatch(memory, err)
		}
		decisions := make(map[string]decision)
		for _, assessment := range report.Decisions {
			if _, duplicate := decisions[assessment.FailureID]; duplicate {
				return monitor.failBatch(memory, fmt.Errorf("duplicate decision %s", assessment.FailureID))
			}
			decisions[assessment.FailureID] = assessment
		}
		for _, entry := range current {
			assessment, ok := decisions[entry.ID]
			if !ok || !validDecision(assessment) {
				return monitor.failBatch(memory, fmt.Errorf("missing or invalid decision for %s", entry.PRKey))
			}

			saved := memory.PRs[entry.PRKey]
			for _, check := range entry.Checks {
				saved.Decisions[check.ID] = assessment
			}
			if assessment.Outcome == "hold" {
				hold := assessment
				saved.Hold = &hold
			}
			monitor.logger.Printf("CI assessed: pr=%s outcome=%s task=%s reason=%s", entry.PRKey, assessment.Outcome, assessment.TaskID, assessment.Reason)
		}
		pending.Applied = true
		if err := monitor.save(memory); err != nil {
			return err
		}
	}
	if pending.SessionID != "" {
		if err := monitor.runtime.Archive(ctx, pending.SessionID); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(pending.Directory); err != nil {
		return err
	}
	memory.Pending = nil
	return monitor.save(memory)
}

func (monitor *Monitor) failBatch(memory *state, cause error) error {
	memory.Pending.Error = cause.Error()
	return errors.Join(cause, monitor.save(memory))
}

func (monitor *Monitor) taskRevision(ctx context.Context, id string) (string, error) {
	raw, err := monitor.runtime.ReadTask(ctx, id)
	if err != nil {
		return "", err
	}
	var result struct {
		Revision string `json:"revision"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", err
	}
	if result.Revision == "" {
		return "", fmt.Errorf("task history has no revision")
	}
	return result.Revision, nil
}

func (monitor *Monitor) ReleaseTask(id string) error {
	if !validTaskID(id) {
		return fmt.Errorf("release-task requires a task UUID")
	}
	memory, err := monitor.load()
	if err != nil {
		return err
	}
	for _, saved := range memory.PRs {
		if saved.Hold != nil && saved.Hold.TaskID == id {
			saved.Hold = nil
			saved.Decisions = make(map[string]decision)
		}
	}
	return monitor.save(memory)
}

func (monitor *Monitor) Retry(ctx context.Context) error {
	memory, err := monitor.load()
	if err != nil {
		return err
	}
	if memory.Pending == nil {
		return nil
	}
	// A failed batch keeps its diagnostics until an explicit retry.
	if memory.Pending.Error == "" && memory.Pending.SessionID != "" {
		return fmt.Errorf("coordinator is still pending; refusing duplicate dispatch")
	}
	if memory.Pending.SessionID != "" {
		if err := monitor.runtime.Archive(ctx, memory.Pending.SessionID); err != nil {
			return err
		}
	}
	if err := os.RemoveAll(memory.Pending.Directory); err != nil {
		return err
	}
	memory.Pending = nil
	return monitor.save(memory)
}

func validDecision(value decision) bool {
	if value.Reason == "" {
		return false
	}
	switch value.Outcome {
	case "wake", "hold":
		return validTaskID(value.TaskID)
	case "wait":
		return validTaskID(value.TaskID) && value.Revision != ""
	case "ignore":
		return true
	}
	return false
}

func (monitor *Monitor) load() (*state, error) {
	memory := &state{Version: 1, PRs: make(map[string]*pullRequestState)}
	data, err := os.ReadFile(monitor.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return memory, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, memory); err != nil {
		return nil, err
	}
	if memory.Version != 1 || memory.PRs == nil {
		return nil, fmt.Errorf("invalid CI state")
	}
	return memory, nil
}

func (monitor *Monitor) save(memory *state) error { return writeJSON(monitor.statePath, memory) }

func writeJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".ci-state-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if _, err = file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}

func prKey(value githubapi.PullRequest) string {
	return fmt.Sprintf("%s#%d", strings.ToLower(value.Repository), value.Number)
}
func matchesAuthor(author string, authors []string) bool {
	for _, candidate := range authors {
		if strings.EqualFold(author, candidate) {
			return true
		}
	}
	return false
}
