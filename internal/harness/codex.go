package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"time"
)

const (
	maxStderrCaptureBytes = 64 * 1024
	processCleanupTimeout = 5 * time.Second
)

type sessionDiscoverer interface {
	Discover(context.Context, []Target) ([]Discovery, error)
}

type commandRunner interface {
	Run(context.Context, string, []string, string, func(string)) error
}

type Codex struct {
	command    string
	args       []string
	runner     commandRunner
	discoverer sessionDiscoverer
}

func NewCodex(command string, args, environment []string) *Codex {
	return &Codex{
		command:    command,
		args:       append([]string(nil), args...),
		runner:     &execRunner{environment: slices.Clone(environment)},
		discoverer: &codexAppServer{command: command, environment: slices.Clone(environment)},
	}
}

func (codex *Codex) Name() string {
	return "codex"
}

func (codex *Codex) Discover(ctx context.Context, targets []Target) ([]Discovery, error) {
	return codex.discoverer.Discover(ctx, targets)
}

func (codex *Codex) Dispatch(ctx context.Context, request Request) (Result, error) {
	args := []string{"exec"}
	if request.SessionID != "" {
		args = append(args, "resume")
	}
	args = append(args, codex.args...)
	args = append(args, "--json")
	if request.SessionID == "" {
		if strings.TrimSpace(request.WorkingDirectory) == "" {
			return Result{}, fmt.Errorf("working directory is required to start a Codex session")
		}
		args = append(args, "-C", request.WorkingDirectory, "-")
	} else {
		if !validSessionID(request.SessionID) {
			return Result{}, fmt.Errorf("invalid Codex session ID %q", request.SessionID)
		}
		args = append(args, request.SessionID, "-")
	}

	sessionID := request.SessionID
	err := codex.runner.Run(ctx, codex.command, args, request.Prompt, func(line string) {
		if sessionID == "" {
			sessionID = sessionIDFromEvent(line)
		}
	})
	if err != nil {
		if request.SessionID != "" && isSessionNotFound(err) {
			return Result{SessionID: sessionID}, fmt.Errorf("%w: %v", ErrSessionNotFound, err)
		}
		return Result{SessionID: sessionID}, fmt.Errorf("run Codex: %w", err)
	}
	if sessionID == "" {
		return Result{}, fmt.Errorf("Codex completed without reporting a session ID")
	}
	return Result{SessionID: sessionID}, nil
}

func validSessionID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return false
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdefABCDEF", character) {
			return false
		}
	}
	return true
}

func sessionIDFromEvent(line string) string {
	var event struct {
		Type     string `json:"type"`
		ThreadID string `json:"thread_id"`
		Thread   struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if json.Unmarshal([]byte(line), &event) != nil || event.Type != "thread.started" {
		return ""
	}
	sessionID := event.ThreadID
	if sessionID == "" {
		sessionID = event.Thread.ID
	}
	if !validSessionID(sessionID) {
		return ""
	}
	return sessionID
}

type execRunner struct {
	environment []string
}

func (runner *execRunner) Run(ctx context.Context, command string, args []string, input string, output func(string)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	process := exec.CommandContext(ctx, command, args...)
	configureProcess(process)
	var cleanupFailed atomic.Bool
	process.Cancel = func() error {
		err := terminateProcessTree(process.Process)
		if err != nil && !errors.Is(err, os.ErrProcessDone) {
			cleanupFailed.Store(true)
		}
		return err
	}
	process.WaitDelay = processCleanupTimeout
	process.Stdin = strings.NewReader(input)
	stderr := &boundedBuffer{limit: maxStderrCaptureBytes}
	process.Stderr = stderr
	process.Env = slices.Clone(runner.environment)
	stdout, err := process.StdoutPipe()
	if err != nil {
		return err
	}
	if err := process.Start(); err != nil {
		return err
	}
	cleanupFinished := make(chan struct{})
	var pipeCleanupTimedOut atomic.Bool
	stopPipeCleanup := context.AfterFunc(ctx, func() {
		timer := time.NewTimer(processCleanupTimeout)
		defer timer.Stop()
		select {
		case <-cleanupFinished:
		case <-timer.C:
			pipeCleanupTimedOut.Store(true)
			_ = stdout.Close()
		}
	})
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		output(scanner.Text())
	}
	scanErr := scanner.Err()
	if scanErr != nil {
		if cancelErr := process.Cancel(); cancelErr != nil && !errors.Is(cancelErr, os.ErrProcessDone) {
			_ = process.Process.Kill()
		}
	}
	waitErr := process.Wait()
	close(cleanupFinished)
	stopPipeCleanup()
	if ctxErr := ctx.Err(); ctxErr != nil {
		if cleanupFailed.Load() {
			return &commandError{cause: fmt.Errorf("%w: Codex process cleanup failed", ctxErr), stderr: stderr.String()}
		}
		if pipeCleanupTimedOut.Load() || errors.Is(waitErr, exec.ErrWaitDelay) {
			return &commandError{cause: fmt.Errorf("%w: Codex process cleanup timed out", ctxErr), stderr: stderr.String()}
		}
		return &commandError{cause: ctxErr, stderr: stderr.String()}
	}
	if scanErr != nil {
		if cleanupFailed.Load() {
			return fmt.Errorf("%w: Codex process cleanup failed", scanErr)
		}
		return scanErr
	}
	if waitErr != nil {
		return &commandError{cause: waitErr, stderr: stderr.String()}
	}
	return nil
}

type commandError struct {
	cause  error
	stderr string
}

func (commandError *commandError) Error() string {
	lowerStderr := strings.ToLower(commandError.stderr)
	for _, marker := range []string{"authentication failed", "not authenticated", "not logged in", "unauthorized"} {
		if strings.Contains(lowerStderr, marker) {
			return fmt.Sprintf("%s: Codex authentication failed", commandError.cause)
		}
	}
	return commandError.cause.Error()
}

func (commandError *commandError) Unwrap() error {
	return commandError.cause
}

func isSessionNotFound(err error) bool {
	var commandError *commandError
	return errors.As(err, &commandError) && strings.Contains(strings.ToLower(commandError.stderr), "no rollout found for thread id")
}

type boundedBuffer struct {
	data  []byte
	limit int
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	written := len(value)
	remaining := buffer.limit - len(buffer.data)
	if remaining > 0 {
		if len(value) > remaining {
			value = value[:remaining]
		}
		buffer.data = append(buffer.data, value...)
	}
	return written, nil
}

func (buffer *boundedBuffer) String() string {
	return string(buffer.data)
}
