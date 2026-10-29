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
	"sync/atomic"
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
	Launch(context.Context, string, []string, string, func(string) bool) error
}

type Codex struct {
	command    string
	args       []string
	runner     commandRunner
	discoverer sessionDiscoverer
}

var _ Harness = (*Codex)(nil)

func NewCodex(command string, args, environment []string) *Codex {
	processes := osProcessStarter{}
	appServer := &codexAppServer{command: command, environment: slices.Clone(environment), processes: processes}
	return &Codex{
		command:    command,
		args:       append([]string(nil), args...),
		runner:     &execRunner{environment: slices.Clone(environment), processes: processes},
		discoverer: appServer,
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
			return Result{SessionID: sessionID}, fmt.Errorf("%w: %w", ErrSessionNotFound, err)
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
	inputFile, outputPath, stderrPath, err := createLaunchFiles(input)
	if err != nil {
		return err
	}
	defer closeAndRemove(inputFile)
	outputFile, err := os.OpenFile(outputPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		_ = os.Remove(outputPath)
		_ = os.Remove(stderrPath)
		return err
	}
	defer closeAndRemove(outputFile)
	stderrFile, err := os.OpenFile(stderrPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		_ = os.Remove(stderrPath)
		return err
	}
	defer closeAndRemove(stderrFile)
	stderr := &cappedFile{file: stderrFile, remaining: maxLaunchOutputBytes}
	process := exec.Command(command, args...)
	configureProcess(process)
	process.Env = slices.Clone(runner.environment)
	process.Stdin = inputFile
	process.Stdout = outputFile
	process.Stderr = stderr
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
			return waitForLaunchExit(process, done, outputErr, stderrPath, stderr)
		}
		if stderr.Truncated() {
			return waitForLaunchExit(process, done, fmt.Errorf("Codex launch stderr exceeded %d bytes before acknowledgement", maxLaunchOutputBytes), stderrPath, stderr)
		}
		if acknowledged {
			if err := launchContext.Err(); err != nil {
				return waitForLaunchExit(process, done, err, stderrPath, stderr)
			}
			go reapLaunchedCommand(done, inputFile.Name(), outputFile.Name(), stderrFile.Name())
			return nil
		}
		select {
		case waitErr := <-done:
			if stderr.Truncated() {
				return commandRunError(processExit{
					WaitError:    waitErr,
					ContextError: fmt.Errorf("Codex launch stderr exceeded %d bytes before acknowledgement", maxLaunchOutputBytes),
					Stderr:       readLaunchStderr(stderrPath, true),
				}, nil)
			}
			return commandRunError(processExit{WaitError: waitErr, Stderr: readLaunchStderr(stderrPath, stderr.Truncated())}, nil)
		case <-launchContext.Done():
			return waitForLaunchExit(process, done, launchContext.Err(), stderrPath, stderr)
		case <-ticker.C:
		}
	}
}

func closeAndRemove(file *os.File) {
	path := file.Name()
	_ = file.Close()
	_ = os.Remove(path)
}

func createLaunchFiles(input string) (*os.File, string, string, error) {
	inputFile, err := os.CreateTemp("", "bifrost-codex-input-*")
	if err != nil {
		return nil, "", "", err
	}
	if _, err := io.WriteString(inputFile, input); err != nil {
		_ = inputFile.Close()
		_ = os.Remove(inputFile.Name())
		return nil, "", "", err
	}
	if _, err := inputFile.Seek(0, io.SeekStart); err != nil {
		_ = inputFile.Close()
		_ = os.Remove(inputFile.Name())
		return nil, "", "", err
	}
	outputFile, err := os.CreateTemp("", "bifrost-codex-output-*")
	if err != nil {
		_ = inputFile.Close()
		_ = os.Remove(inputFile.Name())
		return nil, "", "", err
	}
	outputPath := outputFile.Name()
	_ = outputFile.Close()
	stderrFile, err := os.CreateTemp("", "bifrost-codex-stderr-*")
	if err != nil {
		_ = os.Remove(outputPath)
		_ = inputFile.Close()
		_ = os.Remove(inputFile.Name())
		return nil, "", "", err
	}
	stderrPath := stderrFile.Name()
	_ = stderrFile.Close()
	return inputFile, outputPath, stderrPath, nil
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

func waitForLaunchExit(process *exec.Cmd, done <-chan error, cause error, stderrPath string, stderr *cappedFile) error {
	cleanupErr := terminateCommand(process.Process)
	waitErr := <-done
	return commandRunError(processExit{
		WaitError: waitErr, ContextError: cause, CleanupFailed: cleanupErr != nil, Stderr: readLaunchStderr(stderrPath, stderr.Truncated()),
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

func readLaunchStderr(path string, truncated bool) string {
	output := readLaunchOutput(path)
	if truncated {
		return output + "\n[Codex stderr truncated after 1048576 bytes]"
	}
	return output
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
	if strings.Contains(lowerStderr, "already has an active writer") {
		return fmt.Sprintf("%s: %s", commandError.cause, ErrSessionActiveWriter)
	}
	return commandError.cause.Error()
}

func (commandError *commandError) Unwrap() []error {
	if strings.Contains(strings.ToLower(commandError.stderr), "already has an active writer") {
		return []error{commandError.cause, ErrSessionActiveWriter}
	}
	return []error{commandError.cause}
}

func (commandError *commandError) Diagnostic() string {
	return strings.TrimSpace(commandError.stderr)
}

func isSessionNotFound(err error) bool {
	var commandError *commandError
	return errors.As(err, &commandError) && strings.Contains(strings.ToLower(commandError.stderr), "no rollout found for thread id")
}

type boundedBuffer struct {
	data  []byte
	limit int
}

type cappedFile struct {
	file      *os.File
	remaining int64
	truncated atomic.Bool
}

func (writer *cappedFile) Write(value []byte) (int, error) {
	length := len(value)
	if writer.remaining <= 0 {
		writer.truncated.Store(true)
		return length, nil
	}
	if int64(length) > writer.remaining {
		value = value[:writer.remaining]
		writer.truncated.Store(true)
	}
	written, err := writer.file.Write(value)
	writer.remaining -= int64(written)
	if err != nil || written != len(value) {
		return written, err
	}
	return length, nil
}

func (writer *cappedFile) Truncated() bool {
	return writer.truncated.Load()
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
