package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"slices"
	"strings"
	"unicode"
)

var ErrSessionNotFound = errors.New("Codex session not found")

const (
	maxStderrCaptureBytes = 64 * 1024
	maxDiagnosticBytes    = 4 * 1024
)

var (
	ansiEscapePattern          = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)
	authorizationPattern       = regexp.MustCompile(`(?im)(["']?[a-z0-9_-]*authorization[a-z0-9_-]*["']?)([ \t]*[:=][ \t]*)[^\r\n]*`)
	sensitiveAssignmentPattern = regexp.MustCompile(`(?i)(["']?[a-z0-9_-]*(?:password|secret|token|api[_-]?key|access[_-]?key)[a-z0-9_-]*["']?)([[:space:]]*[:=][[:space:]]*)("[^"\r\n]*"|'[^'\r\n]*'|[^ \t\r\n,;}]+)`)
	credentialPattern          = regexp.MustCompile(`(?i)\b(gh[pousr]_[a-z0-9_]{20,}|github_pat_[a-z0-9_]{20,}|sk-[a-z0-9_-]{20,})\b`)
)

type Request struct {
	SessionID        string
	WorkingDirectory string
	Prompt           string
}

type Result struct {
	SessionID string
}

// Harness implementations must support concurrent Dispatch calls for different sessions.
type Harness interface {
	Name() string
	Dispatch(context.Context, Request) (Result, error)
}

type commandRunner interface {
	Run(context.Context, string, []string, string, func(string)) error
}

type Codex struct {
	command string
	args    []string
	runner  commandRunner
}

func NewCodex(command string, args, environment []string) *Codex {
	return &Codex{
		command: command,
		args:    append([]string(nil), args...),
		runner:  &execRunner{environment: slices.Clone(environment)},
	}
}

func (codex *Codex) Name() string {
	return "codex"
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
	process := exec.CommandContext(ctx, command, args...)
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

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		output(scanner.Text())
	}
	scanErr := scanner.Err()
	if scanErr != nil && process.Process != nil {
		_ = process.Process.Kill()
	}
	waitErr := process.Wait()
	if scanErr != nil {
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
	diagnostic := sanitizeDiagnostic(commandError.stderr)
	if diagnostic == "" {
		return commandError.cause.Error()
	}
	return fmt.Sprintf("%s: %s", commandError.cause, diagnostic)
}

func sanitizeDiagnostic(value string) string {
	value = ansiEscapePattern.ReplaceAllString(value, "")
	value = strings.Map(func(character rune) rune {
		if character != '\r' && character != '\n' && character != '\t' && unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
	value = authorizationPattern.ReplaceAllString(value, "$1$2[redacted]")
	value = sensitiveAssignmentPattern.ReplaceAllString(value, "$1$2[redacted]")
	value = credentialPattern.ReplaceAllString(value, "[redacted]")
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= maxDiagnosticBytes {
		return value
	}
	return strings.ToValidUTF8(value[:maxDiagnosticBytes], "")
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
