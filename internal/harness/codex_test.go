package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"

	githubapi "github.com/matan-yadgar/bifrost/internal/github"
)

type fakeRunner struct {
	command string
	args    []string
	input   string
	lines   []string
	err     error
}

func (runner *fakeRunner) Run(_ context.Context, command string, args []string, input string, output func(string)) error {
	runner.command = command
	runner.args = append([]string(nil), args...)
	runner.input = input
	for _, line := range runner.lines {
		output(line)
	}
	return runner.err
}

func TestCodexDispatchStartsAndResumesSessions(t *testing.T) {
	t.Parallel()
	const sessionID = "019c0000-0000-7000-8000-000000000001"
	runner := &fakeRunner{lines: []string{`{"type":"thread.started","thread_id":"` + sessionID + `"}`}}
	codex := NewCodex("custom-codex", []string{"--approve-for-me"}, nil)
	codex.runner = runner

	result, err := codex.Dispatch(context.Background(), Request{WorkingDirectory: "/repo", Prompt: "handle review"})
	if err != nil {
		t.Fatal(err)
	}
	if result.SessionID != sessionID {
		t.Fatalf("session ID = %q", result.SessionID)
	}
	wantStart := []string{"exec", "--approve-for-me", "--json", "-C", "/repo", "-"}
	if runner.command != "custom-codex" || !reflect.DeepEqual(runner.args, wantStart) || runner.input != "handle review" {
		t.Fatalf("start command/args/input = %q / %#v / %q", runner.command, runner.args, runner.input)
	}

	runner.lines = nil
	result, err = codex.Dispatch(context.Background(), Request{SessionID: sessionID, Prompt: "more review"})
	if err != nil {
		t.Fatal(err)
	}
	wantResume := []string{"exec", "resume", "--approve-for-me", "--json", sessionID, "-"}
	if result.SessionID != sessionID || !reflect.DeepEqual(runner.args, wantResume) {
		t.Fatalf("resume result/args = %#v / %#v", result, runner.args)
	}
}

func TestCodexReturnsStartedSessionWhenRunFails(t *testing.T) {
	t.Parallel()
	const sessionID = "019c0000-0000-7000-8000-000000000002"
	runner := &fakeRunner{
		lines: []string{`{"type":"thread.started","thread_id":"` + sessionID + `"}`},
		err:   errors.New("failed"),
	}
	codex := NewCodex("codex", nil, nil)
	codex.runner = runner

	result, err := codex.Dispatch(context.Background(), Request{WorkingDirectory: "/repo", Prompt: "review"})
	if err == nil || result.SessionID != sessionID {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
}

func TestCodexRejectsInvalidMappedSession(t *testing.T) {
	t.Parallel()
	codex := NewCodex("codex", nil, nil)
	codex.runner = &fakeRunner{}
	if _, err := codex.Dispatch(context.Background(), Request{SessionID: "--last", Prompt: "review"}); err == nil {
		t.Fatal("expected invalid session ID error")
	}
}

func TestCodexClassifiesMissingMappedSession(t *testing.T) {
	t.Parallel()
	const sessionID = "019c0000-0000-7000-8000-000000000003"
	codex := NewCodex("codex", nil, nil)
	codex.runner = &fakeRunner{err: &commandError{
		cause:  errors.New("exit status 1"),
		stderr: "thread/resume failed: no rollout found for thread id " + sessionID,
	}}

	_, err := codex.Dispatch(context.Background(), Request{SessionID: sessionID, Prompt: "review"})
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("error = %v", err)
	}
}

func TestCommandErrorIncludesSanitizedDiagnostic(t *testing.T) {
	t.Parallel()
	err := &commandError{
		cause:  errors.New("exit status 1"),
		stderr: "authentication failed\n\x1b[31mAuthorization\x1b[0m: Basic arbitrary-value\n\"Authorization\":\"Bearer json-auth\"\nHTTP_AUTHORIZATION=Basic prefixed-auth\nJIRA_API_TOKEN=super-secret-value DB_PASSWORD=db-secret \"token\":\"json-secret\" TOKEN\n=split-secret\ncredential ghp_abcdefghijklmnopqrstuvwxyz123456\x1b[31m",
	}
	message := err.Error()
	if !strings.Contains(message, "authentication failed") || !strings.Contains(message, "[redacted]") {
		t.Fatalf("diagnostic = %q", message)
	}
	for _, credential := range []string{"arbitrary-value", "json-auth", "prefixed-auth", "super-secret-value", "db-secret", "json-secret", "split-secret", "ghp_abcdefghijklmnopqrstuvwxyz123456", "\x1b", "[31m"} {
		if strings.Contains(message, credential) {
			t.Fatalf("diagnostic leaked credential or control character = %q", message)
		}
	}
	if !strings.Contains(message, "JIRA_API_TOKEN=[redacted]") || !strings.Contains(message, "DB_PASSWORD=[redacted]") {
		t.Fatalf("diagnostic leaked credential = %q", message)
	}
}

func TestSanitizeDiagnosticBoundsValidUTF8(t *testing.T) {
	t.Parallel()
	diagnostic := sanitizeDiagnostic("useful prefix " + strings.Repeat("界", maxDiagnosticBytes))
	if len(diagnostic) > maxDiagnosticBytes || !utf8.ValidString(diagnostic) {
		t.Fatalf("diagnostic length/UTF-8 = %d / %t", len(diagnostic), utf8.ValidString(diagnostic))
	}
	if !strings.HasPrefix(diagnostic, "useful prefix") {
		t.Fatalf("diagnostic lost useful prefix: %q", diagnostic[:min(len(diagnostic), 64)])
	}
}

func TestSessionIDFromEventRejectsInvalidID(t *testing.T) {
	t.Parallel()
	if got := sessionIDFromEvent(`{"type":"thread.started","thread_id":"--last"}`); got != "" {
		t.Fatalf("session ID = %q", got)
	}
}

func TestExecRunnerUsesSanitizedEnvironment(t *testing.T) {
	t.Parallel()
	environment := githubapi.WithoutAuthTokens([]string{
		"GO_WANT_BIFROST_HELPER=1",
		"BIFROST_SENTINEL=kept",
		"GH_TOKEN=one",
		"GITHUB_TOKEN=two",
	})
	var lines []string
	runner := &execRunner{environment: environment}
	if err := runner.Run(context.Background(), os.Args[0], []string{"-test.run=TestExecRunnerEnvironmentHelper"}, "", func(line string) {
		lines = append(lines, line)
	}); err != nil {
		t.Fatal(err)
	}
	output := strings.Join(lines, "\n")
	if !strings.Contains(output, "BIFROST_SENTINEL=kept") || !strings.Contains(output, "GH_TOKEN_PRESENT=false") || !strings.Contains(output, "GITHUB_TOKEN_PRESENT=false") {
		t.Fatalf("child environment = %q", output)
	}
}

func TestExecRunnerHonorsEmptyEnvironment(t *testing.T) {
	t.Parallel()
	var lines []string
	runner := &execRunner{environment: []string{}}
	if err := runner.Run(context.Background(), "/usr/bin/env", nil, "", func(line string) {
		lines = append(lines, line)
	}); err != nil {
		t.Fatal(err)
	}
	if len(lines) != 0 {
		t.Fatalf("child inherited environment: %#v", lines)
	}
}

func TestExecRunnerEnvironmentHelper(t *testing.T) {
	if os.Getenv("GO_WANT_BIFROST_HELPER") != "1" {
		return
	}
	fmt.Println("BIFROST_SENTINEL=" + os.Getenv("BIFROST_SENTINEL"))
	_, ghTokenPresent := os.LookupEnv("GH_TOKEN")
	_, githubTokenPresent := os.LookupEnv("GITHUB_TOKEN")
	fmt.Printf("GH_TOKEN_PRESENT=%t\n", ghTokenPresent)
	fmt.Printf("GITHUB_TOKEN_PRESENT=%t\n", githubTokenPresent)
	os.Exit(0)
}
