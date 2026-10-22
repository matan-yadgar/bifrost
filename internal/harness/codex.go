package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"
)

const (
	codexLaunchTimeout         = 30 * time.Second
	launchPollInterval         = 25 * time.Millisecond
	maxLaunchOutputBytes int64 = 1024 * 1024
)

type sessionDiscoverer interface {
	Discover(context.Context, []Target) ([]Discovery, error)
}

type commandRunner interface {
	Run(context.Context, string, []string, string, func(string)) error
	Launch(context.Context, string, []string, string, func(string) bool) error
}

type Codex struct {
	command    string
	args       []string
	runner     commandRunner
	discoverer sessionDiscoverer
	appServer  *codexAppServer
}

var _ TaskHarness = (*Codex)(nil)

func NewCodex(command string, args, environment []string) *Codex {
	processes := osProcessStarter{}
	appServer := &codexAppServer{command: command, environment: slices.Clone(environment), processes: processes}
	return &Codex{
		command:    command,
		args:       append([]string(nil), args...),
		runner:     &execRunner{environment: slices.Clone(environment), processes: processes},
		discoverer: appServer,
		appServer:  appServer,
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
	args = append(args, codex.args...)
	if request.SessionID != "" {
		args = append(args, "resume")
	}
	if request.AllowNonRepository {
		args = append(args, "--skip-git-repo-check")
	}
	if request.OutputSchemaPath != "" {
		args = append(args, "--output-schema", request.OutputSchemaPath)
	}
	if request.OutputPath != "" {
		args = append(args, "-o", request.OutputPath)
	}
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
	err := codex.runner.Launch(ctx, codex.command, args, request.Prompt, func(line string) bool {
		reportedSessionID := sessionIDFromEvent(line)
		if reportedSessionID == "" {
			return false
		}
		if sessionID != "" && !strings.EqualFold(sessionID, reportedSessionID) {
			return false
		}
		sessionID = reportedSessionID
		return true
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
	processes   processStarter
}

// Launch returns once Codex acknowledges the thread. Its stdio is file-backed
// so the task can continue when a one-shot Bifrost process exits.
func (runner *execRunner) Launch(ctx context.Context, command string, args []string, input string, started func(string) bool) error {
	launchContext, cancel := context.WithTimeout(ctx, codexLaunchTimeout)
	defer cancel()
	if err := launchContext.Err(); err != nil {
		return err
	}
	inputFile, outputPath, err := createLaunchFiles(input)
	if err != nil {
		return err
	}
	defer closeAndRemove(inputFile)
	outputFile, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		_ = os.Remove(outputPath)
		return err
	}
	defer closeAndRemove(outputFile)
	process := exec.Command(command, args...)
	configureProcess(process)
	process.Env = slices.Clone(runner.environment)
	process.Stdin = inputFile
	process.Stdout = outputFile
	process.Stderr = outputFile
	if err := process.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	ticker := time.NewTicker(launchPollInterval)
	defer ticker.Stop()
	for {
		acknowledged, outputErr := launchAcknowledged(outputPath, started)
		if outputErr != nil {
			return waitForLaunchExit(process, done, outputErr, outputPath)
		}
		if acknowledged {
			if err := launchContext.Err(); err != nil {
				return waitForLaunchExit(process, done, err, outputPath)
			}
			go reapLaunchedCommand(done, inputFile.Name(), outputFile.Name())
			return nil
		}
		select {
		case waitErr := <-done:
			return commandRunError(processExit{WaitError: waitErr, Stderr: readLaunchOutput(outputPath)}, nil)
		case <-launchContext.Done():
			return waitForLaunchExit(process, done, launchContext.Err(), outputPath)
		case <-ticker.C:
		}
	}
}

func closeAndRemove(file *os.File) {
	path := file.Name()
	_ = file.Close()
	_ = os.Remove(path)
}

func createLaunchFiles(input string) (*os.File, string, error) {
	inputFile, err := os.CreateTemp("", "bifrost-codex-input-*")
	if err != nil {
		return nil, "", err
	}
	if _, err := io.WriteString(inputFile, input); err != nil {
		_ = inputFile.Close()
		_ = os.Remove(inputFile.Name())
		return nil, "", err
	}
	if _, err := inputFile.Seek(0, io.SeekStart); err != nil {
		_ = inputFile.Close()
		_ = os.Remove(inputFile.Name())
		return nil, "", err
	}
	outputFile, err := os.CreateTemp("", "bifrost-codex-output-*")
	if err != nil {
		_ = inputFile.Close()
		_ = os.Remove(inputFile.Name())
		return nil, "", err
	}
	outputPath := outputFile.Name()
	_ = outputFile.Close()
	return inputFile, outputPath, nil
}

func launchAcknowledged(outputPath string, started func(string) bool) (bool, error) {
	output, err := readLaunchOutputBytes(outputPath)
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		if started(line) {
			return true, nil
		}
	}
	if int64(len(output)) > maxLaunchOutputBytes {
		return false, fmt.Errorf("Codex launch output exceeded %d bytes before acknowledgement", maxLaunchOutputBytes)
	}
	return false, nil
}

func waitForLaunchExit(process *exec.Cmd, done <-chan error, cause error, stderrPath string) error {
	cleanupErr := terminateCommand(process.Process)
	waitErr := <-done
	return commandRunError(processExit{
		WaitError: waitErr, ContextError: cause, CleanupFailed: cleanupErr != nil, Stderr: readLaunchOutput(stderrPath),
	}, nil)
}

func reapLaunchedCommand(done <-chan error, paths ...string) {
	<-done
	for _, path := range paths {
		_ = os.Remove(path)
	}
}

func readLaunchOutput(path string) string {
	output, err := readLaunchOutputBytes(path)
	if err != nil {
		return ""
	}
	return string(output)
}

func readLaunchOutputBytes(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	return io.ReadAll(io.LimitReader(file, maxLaunchOutputBytes+1))
}

func (runner *execRunner) Run(ctx context.Context, command string, args []string, input string, output func(string)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	process, err := runner.processes.Start(ctx, processRequest{
		Command: command, Args: args, Environment: runner.environment, Input: strings.NewReader(input),
	})
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(process.Output())
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		output(scanner.Text())
	}
	scanErr := scanner.Err()
	if scanErr != nil {
		process.Cancel()
	}
	return commandRunError(process.Wait(), scanErr)
}

func commandRunError(exit processExit, scanErr error) error {
	if exit.ContextError != nil {
		if exit.CleanupFailed {
			return &commandError{cause: fmt.Errorf("%w: Codex process cleanup failed", exit.ContextError), stderr: exit.Stderr}
		}
		if exit.PipeCleanupTimedOut || errors.Is(exit.WaitError, exec.ErrWaitDelay) {
			return &commandError{cause: fmt.Errorf("%w: Codex process cleanup timed out", exit.ContextError), stderr: exit.Stderr}
		}
		return &commandError{cause: exit.ContextError, stderr: exit.Stderr}
	}
	if scanErr != nil {
		if exit.CleanupFailed {
			return fmt.Errorf("%w: Codex process cleanup failed", scanErr)
		}
		return scanErr
	}
	if exit.WaitError != nil {
		return &commandError{cause: exit.WaitError, stderr: exit.Stderr}
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
