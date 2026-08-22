package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	githubapi "github.com/matan-yadgar/bifrost/internal/github"
)

const appServerHelperEnvironment = "GO_WANT_BIFROST_APP_SERVER_HELPER"

func TestMain(testingMain *testing.M) {
	if mode := os.Getenv(appServerHelperEnvironment); mode != "" {
		runAppServerHelper(mode)
		return
	}
	os.Exit(testingMain.Run())
}

func TestCodexDiscoveryOwnsAppServerProcess(t *testing.T) {
	const sessionID = "019c0000-0000-7000-8000-000000000009"
	target := Target{URL: "https://github.com/owner/repo/pull/42", HeadRef: "codex/feature-42"}
	readyPath := filepath.Join(t.TempDir(), "app-server-ready")
	startsPath := filepath.Join(t.TempDir(), "app-server-starts")
	newCodex := func(mode string) *Codex {
		return NewCodex(os.Args[0], nil, []string{
			appServerHelperEnvironment + "=" + mode,
			"BIFROST_APP_SERVER_SENTINEL=expected",
			"BIFROST_APP_SERVER_READY=" + readyPath,
			"BIFROST_APP_SERVER_STARTS=" + startsPath,
		})
	}

	discoveries, err := newCodex("happy").Discover(context.Background(), []Target{target})
	if err != nil {
		t.Fatal(err)
	}
	if len(discoveries) != 1 || !discoveries[0].Found || discoveries[0].Session.ID != sessionID {
		t.Fatalf("discoveries = %#v", discoveries)
	}

	if err := os.Remove(readyPath); err != nil {
		t.Fatal(err)
	}
	canceledContext, cancel := context.WithCancel(context.Background())
	cancelResult := make(chan error, 1)
	started := time.Now()
	go func() {
		_, discoveryError := newCodex("hang").Discover(canceledContext, []Target{target})
		cancelResult <- discoveryError
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, statError := os.Stat(readyPath); statError == nil {
			break
		} else if !errors.Is(statError, os.ErrNotExist) {
			t.Fatal(statError)
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("helper did not report readiness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case err = <-cancelResult:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled discovery did not return")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("canceled discovery took %s", elapsed)
	}

	_, err = newCodex("failure").Discover(context.Background(), []Target{target})
	if err == nil || strings.Contains(err.Error(), "private helper diagnostic") {
		t.Fatalf("failure error = %v", err)
	}

	discoveries, err = newCodex("partial").Discover(context.Background(), []Target{
		{URL: "https://github.com/owner/repo/pull/41", HeadRef: "codex/feature-41"},
		target,
	})
	if err != nil || len(discoveries) != 2 || discoveries[0].Err == nil || !discoveries[1].Found || discoveries[1].Session.ID != sessionID {
		t.Fatalf("partial discoveries/error = %#v / %v", discoveries, err)
	}

	discoveries, err = newCodex("fatal-partial").Discover(context.Background(), []Target{
		target,
		{URL: "https://github.com/owner/repo/pull/41", HeadRef: "codex/feature-41"},
		target,
	})
	if err != nil || len(discoveries) != 3 || !discoveries[0].Found || discoveries[1].Err == nil || !discoveries[2].Found {
		t.Fatalf("fatal partial discoveries/error = %#v / %v", discoveries, err)
	}
	startsBefore, err := os.ReadFile(startsPath)
	if err != nil {
		t.Fatal(err)
	}
	discoveries, err = newCodex("fatal-last").Discover(context.Background(), []Target{
		target,
		{URL: "https://github.com/owner/repo/pull/41", HeadRef: "codex/feature-41"},
	})
	startsAfter, readError := os.ReadFile(startsPath)
	if readError != nil || err != nil || len(discoveries) != 2 || !discoveries[0].Found || discoveries[1].Err == nil || len(startsAfter) != len(startsBefore)+1 {
		t.Fatalf("fatal-last discoveries/error/starts = %#v / %v / %d->%d", discoveries, err, len(startsBefore), len(startsAfter))
	}
}

func TestCodexDiscoveryUsesManagedProcessTransport(t *testing.T) {
	t.Parallel()
	const sessionID = "019c0000-0000-7000-8000-000000000009"
	process := &fakeManagedProcess{
		input: &writeCloserBuffer{},
		output: io.NopCloser(strings.NewReader(
			rpcResult(1, `{}`) +
				rpcResult(2, searchResult(nil, "")) +
				rpcResult(3, searchResult([]string{sessionID}, "")) +
				rpcResult(4, threadFullTurnsResultJSON("turn-1", "https://github.com/owner/repo/pull/42 codex/feature-42", stringPointer(finalAssistantMessage))),
		)),
	}
	starter := &fakeProcessStarter{process: process}
	server := &codexAppServer{command: "codex", environment: []string{"SAFE=value"}, processes: starter}

	discoveries, err := server.Discover(context.Background(), []Target{{
		URL: "https://github.com/owner/repo/pull/42", HeadRef: "codex/feature-42",
	}})
	if err != nil || len(discoveries) != 1 || !discoveries[0].Found || discoveries[0].Session.ID != sessionID {
		t.Fatalf("discoveries/error = %#v / %v", discoveries, err)
	}
	if starter.request.Command != "codex" || !reflect.DeepEqual(starter.request.Args, []string{"app-server", "--stdio"}) ||
		!starter.request.Interactive || !reflect.DeepEqual(starter.request.Environment, []string{"SAFE=value"}) {
		t.Fatalf("process request = %#v", starter.request)
	}
	if !process.input.closed {
		t.Fatal("app-server input was not closed")
	}
	if !process.waited {
		t.Fatal("app-server process was not reaped")
	}
}

func runAppServerHelper(mode string) {
	if !reflect.DeepEqual(os.Args[1:], []string{"app-server", "--stdio"}) || os.Getenv("BIFROST_APP_SERVER_SENTINEL") != "expected" {
		os.Exit(11)
	}
	if _, found := os.LookupEnv("GH_TOKEN"); found {
		os.Exit(12)
	}
	if readyPath := os.Getenv("BIFROST_APP_SERVER_READY"); readyPath != "" {
		if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
			os.Exit(16)
		}
	}
	if startsPath := os.Getenv("BIFROST_APP_SERVER_STARTS"); startsPath != "" {
		file, err := os.OpenFile(startsPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			os.Exit(17)
		}
		if _, err := file.WriteString("x"); err != nil || file.Close() != nil {
			os.Exit(18)
		}
	}
	if mode == "hang" {
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}
	const sessionID = "019c0000-0000-7000-8000-000000000009"
	decoder := json.NewDecoder(os.Stdin)
	encoder := json.NewEncoder(os.Stdout)
	for {
		var request appServerRequest
		if err := decoder.Decode(&request); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			os.Exit(13)
		}
		switch request.Method {
		case "initialize":
			_ = encoder.Encode(map[string]any{"id": request.ID, "result": map[string]any{}})
		case "initialized":
		case "thread/search":
			if mode == "partial" && strings.Contains(fmt.Sprint(request.Params["searchTerm"]), "/pull/41") {
				_ = encoder.Encode(map[string]any{"id": request.ID, "error": map[string]any{"code": -32000}})
				continue
			}
			if strings.HasPrefix(mode, "fatal-") && strings.Contains(fmt.Sprint(request.Params["searchTerm"]), "/pull/41") {
				_ = os.Stdout.Close()
				_, _ = io.Copy(io.Discard, os.Stdin)
				os.Exit(15)
			}
			data := []any{}
			if request.Params["archived"] == true {
				data = append(data, map[string]any{"thread": map[string]string{"id": sessionID}})
			}
			_ = encoder.Encode(map[string]any{"id": request.ID, "result": map[string]any{"data": data, "nextCursor": nil}})
		case "thread/turns/list":
			turn := map[string]any{"id": "turn-1", "status": "completed", "itemsView": request.Params["itemsView"]}
			if request.Params["itemsView"] == "full" {
				text := "created https://github.com/owner/repo/pull/42 from codex/feature-42"
				turn["items"] = []any{map[string]any{"type": agentMessageItem, "phase": finalAssistantMessage, "text": text}}
			}
			result := map[string]any{"data": []any{turn}, "nextCursor": nil}
			_ = encoder.Encode(map[string]any{"id": request.ID, "result": result})
		case "thread/items/list":
			text := "created https://github.com/owner/repo/pull/42 from codex/feature-42"
			result := map[string]any{"data": []any{map[string]any{
				"turnId": "turn-1",
				"item":   map[string]any{"type": agentMessageItem, "phase": finalAssistantMessage, "text": text},
			}}, "nextCursor": nil}
			_ = encoder.Encode(map[string]any{"id": request.ID, "result": result})
		default:
			os.Exit(14)
		}
	}
	if mode == "failure" {
		fmt.Fprintln(os.Stderr, "private helper diagnostic")
		os.Exit(7)
	}
	os.Exit(0)
}

func TestAppServerDiscoveryUsesURLCandidatesAcrossPages(t *testing.T) {
	t.Parallel()
	const creatorSessionID = "019c0000-0000-7000-8000-000000000001"
	const reviewerSessionID = "019c0000-0000-7000-8000-000000000002"
	responses := strings.NewReader(
		rpcResult(1, `{}`) +
			rpcResult(2, searchResult([]string{reviewerSessionID}, "next")) +
			rpcResult(3, searchResult(nil, "")) +
			rpcResult(4, searchResult([]string{creatorSessionID}, "")) +
			rpcResult(5, threadFullTurnsResultJSON("turn-1", "created https://github.com/owner/repo/pull/42 from codex/feature-42", stringPointer(finalAssistantMessage))) +
			rpcResult(6, threadFullTurnsResultJSON("turn-2", "reviewed https://github.com/owner/repo/pull/42", stringPointer(finalAssistantMessage))),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)

	if err := client.initialize(); err != nil {
		t.Fatal(err)
	}
	discovery := client.discover(Target{URL: "https://github.com/owner/repo/pull/42", HeadRef: "codex/feature-42"})
	if discovery.Err != nil {
		t.Fatal(discovery.Err)
	}
	if !discovery.Found || discovery.Session.ID != creatorSessionID {
		t.Fatalf("discovery = %#v", discovery)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 7 {
		t.Fatalf("request count = %d: %#v", len(emitted), emitted)
	}
	wantMethods := []string{"initialize", "initialized", "thread/search", "thread/search", "thread/search", "thread/turns/list", "thread/turns/list"}
	for index, method := range wantMethods {
		if emitted[index].Method != method {
			t.Fatalf("request %d method = %q", index, emitted[index].Method)
		}
	}
	assertExactProtocolRequests(t, emitted, creatorSessionID)
}

func TestAppServerDiscoveryRejectsAmbiguousURLCandidates(t *testing.T) {
	t.Parallel()
	const firstSessionID = "019c0000-0000-7000-8000-000000000001"
	const secondSessionID = "019c0000-0000-7000-8000-000000000002"
	const unreadableSessionID = "019c0000-0000-7000-8000-000000000003"
	sessions := []string{firstSessionID, secondSessionID, unreadableSessionID}
	responses := strings.NewReader(
		rpcResult(1, `{}`) +
			rpcResult(2, searchResult(sessions, "")) + rpcResult(3, searchResult(nil, "")) +
			rpcResult(4, threadFullTurnsResultJSON("turn-1", "https://github.com/owner/repo/pull/42 codex/feature-42", stringPointer(finalAssistantMessage))) +
			rpcResult(5, threadFullTurnsResultJSON("turn-2", "https://github.com/owner/repo/pull/42 codex/feature-42", stringPointer(finalAssistantMessage))),
	)
	client := newAppServerClient(responses, &bytes.Buffer{})
	if err := client.initialize(); err != nil {
		t.Fatal(err)
	}
	discovery := client.discover(Target{URL: "https://github.com/owner/repo/pull/42", HeadRef: "codex/feature-42"})
	if !errors.Is(discovery.Err, ErrAmbiguousSession) {
		t.Fatalf("error = %v", discovery.Err)
	}
}

func TestAppServerDiscoveryExcludesKnownStaleSessionBeforeAmbiguity(t *testing.T) {
	t.Parallel()
	const olderStaleSessionID = "019c0000-0000-7000-8000-000000000000"
	const staleSessionID = "019c0000-0000-7000-8000-000000000001"
	const replacementSessionID = "019c0000-0000-7000-8000-000000000002"
	responses := strings.NewReader(
		rpcResult(1, `{}`) +
			rpcResult(2, searchResult([]string{olderStaleSessionID, staleSessionID, replacementSessionID}, "")) +
			rpcResult(3, searchResult(nil, "")) +
			rpcResult(4, threadFullTurnsResultJSON("turn-2", "https://github.com/owner/repo/pull/42 codex/feature-42", stringPointer(finalAssistantMessage))),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)
	if err := client.initialize(); err != nil {
		t.Fatal(err)
	}
	discovery := client.discover(Target{
		URL: "https://github.com/owner/repo/pull/42", HeadRef: "codex/feature-42",
		ExcludedSessionIDs: []string{olderStaleSessionID, staleSessionID},
	})
	if discovery.Err != nil || !discovery.Found || discovery.Session.ID != replacementSessionID {
		t.Fatalf("discovery = %#v", discovery)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 5 || emitted[4].Params["threadId"] != replacementSessionID {
		t.Fatalf("requests = %#v", emitted)
	}
}

func TestAppServerDiscoveryRequiresOneFinalResponseWithBothIdentifiers(t *testing.T) {
	t.Parallel()
	const sessionID = "019c0000-0000-7000-8000-000000000003"
	turnsResult := `{"data":[{"id":"turn-1","status":"completed","items":[` +
		`{"type":"agentMessage","phase":"final_answer","text":"https://github.com/owner/repo/pull/42"},` +
		`{"type":"agentMessage","phase":"final_answer","text":"codex/feature-42"},` +
		`{"type":"agentMessage","phase":"commentary","text":"https://github.com/owner/repo/pull/42 codex/feature-42"}` +
		`]}],"nextCursor":null}`
	responses := strings.NewReader(
		rpcResult(1, `{}`) +
			rpcResult(2, searchResult([]string{sessionID}, "")) + rpcResult(3, searchResult(nil, "")) +
			rpcResult(4, turnsResult),
	)
	client := newAppServerClient(responses, &bytes.Buffer{})
	if err := client.initialize(); err != nil {
		t.Fatal(err)
	}
	discovery := client.discover(Target{URL: "https://github.com/owner/repo/pull/42", HeadRef: "codex/feature-42"})
	if discovery.Err != nil || discovery.Found {
		t.Fatalf("discovery = %#v", discovery)
	}
}

func TestCreatorFinalUsesLegacyTerminalMessageAndExactBoundaries(t *testing.T) {
	t.Parallel()
	const pullRequestURL = "https://github.com/owner/repo/pull/42"
	const headRef = "codex/feature-42"
	for _, testCase := range []struct {
		name      string
		itemsJSON string
		want      bool
	}{
		{
			name:      "legacy terminal answer",
			itemsJSON: threadItemsResultJSON("turn-1", "created "+pullRequestURL+" from "+headRef, nil),
			want:      true,
		},
		{
			name: "legacy nonterminal answer",
			itemsJSON: `{"data":[` +
				`{"turnId":"turn-1","item":{"type":"agentMessage","phase":null,"text":"created ` + pullRequestURL + ` from ` + headRef + `"}},` +
				`{"turnId":"turn-1","item":{"type":"agentMessage","phase":null,"text":"later answer"}}` +
				`],"nextCursor":null}`,
			want: false,
		},
		{
			name:      "identifier prefixes",
			itemsJSON: threadItemsResultJSON("turn-1", "created "+pullRequestURL+"0 from "+headRef+"-next", stringPointer(finalAssistantMessage)),
			want:      false,
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			client := newAppServerClient(strings.NewReader(rpcResult(1, testCase.itemsJSON)), &bytes.Buffer{})
			got, err := client.creatorFinalInItems("session", map[string]bool{"turn-1": true}, pullRequestURL, headRef)
			if err != nil || got != testCase.want {
				t.Fatalf("creatorFinalInItems() = %v, %v; want %v", got, err, testCase.want)
			}
		})
	}
}

func TestAppServerCreatorFinalPaginatesTurns(t *testing.T) {
	t.Parallel()
	responses := strings.NewReader(
		rpcError(1, -32600, "itemsView full is unavailable for paginated history") +
			rpcResult(2, `{"data":[{"id":"turn-1","status":"completed"}],"nextCursor":"older"}`) +
			rpcResult(3, threadTurnsResultJSON("turn-2")) +
			rpcResult(4, `{"data":[],"nextCursor":"older-items"}`) +
			rpcResult(5, threadItemsResultJSON("turn-2", "https://github.com/owner/repo/pull/42 codex/feature-42", stringPointer(finalAssistantMessage))),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)
	qualified, err := client.hasCreatorFinal("019c0000-0000-7000-8000-000000000003", "https://github.com/owner/repo/pull/42", "codex/feature-42")
	if err != nil || !qualified {
		t.Fatalf("qualified/error = %v / %v", qualified, err)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 5 || emitted[0].Params["itemsView"] != "full" || emitted[1].Params["itemsView"] != "notLoaded" || emitted[2].Params["cursor"] != "older" || emitted[4].Params["cursor"] != "older-items" {
		t.Fatalf("turn requests = %#v", emitted)
	}
}

func TestCreatorFinalInFullTurnsPaginatesPrimaryPath(t *testing.T) {
	t.Parallel()
	responses := strings.NewReader(
		rpcResult(1, `{"data":[{"id":"turn-2","status":"completed","itemsView":"full","items":[]}],"nextCursor":"older"}`) +
			rpcResult(2, threadFullTurnsResultJSON("turn-1", "https://github.com/owner/repo/pull/42 codex/feature-42", stringPointer(finalAssistantMessage))),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)
	qualified, err := client.creatorFinalInFullTurns("session", "https://github.com/owner/repo/pull/42", "codex/feature-42")
	if err != nil || !qualified {
		t.Fatalf("creatorFinalInFullTurns() = %v, %v", qualified, err)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 2 || emitted[0].Params["itemsView"] != "full" || emitted[1].Params["itemsView"] != "full" || emitted[1].Params["cursor"] != "older" {
		t.Fatalf("turn requests = %#v", emitted)
	}
}

func TestCreatorFinalInFullTurnsAcceptsLegacyTerminalMessage(t *testing.T) {
	t.Parallel()
	const pullRequestURL = "https://github.com/owner/repo/pull/42"
	const headRef = "codex/feature-42"
	response := `{"data":[{"id":"turn-1","status":"completed","itemsView":"full","items":[` +
		`{"type":"agentMessage","phase":null,"text":"earlier"},` +
		`{"type":"agentMessage","phase":null,"text":"created ` + pullRequestURL + ` from ` + headRef + `"}` +
		`]}],"nextCursor":null}`
	client := newAppServerClient(strings.NewReader(rpcResult(1, response)), &bytes.Buffer{})
	qualified, err := client.creatorFinalInFullTurns("session", pullRequestURL, headRef)
	if err != nil || !qualified {
		t.Fatalf("creatorFinalInFullTurns() = %v, %v", qualified, err)
	}
}

func TestCreatorFinalInFullTurnsRejectsLegacyNonterminalMessage(t *testing.T) {
	t.Parallel()
	const pullRequestURL = "https://github.com/owner/repo/pull/42"
	const headRef = "codex/feature-42"
	response := `{"data":[{"id":"turn-1","status":"completed","itemsView":"full","items":[` +
		`{"type":"agentMessage","phase":null,"text":"created ` + pullRequestURL + ` from ` + headRef + `"},` +
		`{"type":"agentMessage","phase":null,"text":"later answer"}` +
		`]}],"nextCursor":null}`
	client := newAppServerClient(strings.NewReader(rpcResult(1, response)), &bytes.Buffer{})
	qualified, err := client.creatorFinalInFullTurns("session", pullRequestURL, headRef)
	if err != nil || qualified {
		t.Fatalf("creatorFinalInFullTurns() = %v, %v", qualified, err)
	}
}

func TestCreatorFinalInFullTurnsRequiresExactIdentifierBoundaries(t *testing.T) {
	t.Parallel()
	const pullRequestURL = "https://github.com/owner/repo/pull/42"
	const headRef = "codex/feature-42"
	for _, testCase := range []struct {
		name string
		text string
	}{
		{name: "before URL", text: "x" + pullRequestURL + " " + headRef},
		{name: "after URL", text: pullRequestURL + "0 " + headRef},
		{name: "before branch", text: pullRequestURL + " x" + headRef},
		{name: "after branch", text: pullRequestURL + " " + headRef + "-next"},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			response := threadFullTurnsResultJSON("turn-1", testCase.text, stringPointer(finalAssistantMessage))
			client := newAppServerClient(strings.NewReader(rpcResult(1, response)), &bytes.Buffer{})
			qualified, err := client.creatorFinalInFullTurns("session", pullRequestURL, headRef)
			if err != nil || qualified {
				t.Fatalf("creatorFinalInFullTurns() = %v, %v", qualified, err)
			}
		})
	}
}

func TestAppServerCreatorFinalFallsBackWhenFullItemsAreUnloaded(t *testing.T) {
	t.Parallel()
	responses := strings.NewReader(
		rpcResult(1, `{"data":[{"id":"turn-1","status":"completed","items":[],"itemsView":"notLoaded"}],"nextCursor":null}`) +
			rpcResult(2, threadTurnsResultJSON("turn-1")) +
			rpcResult(3, threadItemsResultJSON("turn-1", "https://github.com/owner/repo/pull/42 codex/feature-42", stringPointer(finalAssistantMessage))),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)
	qualified, err := client.hasCreatorFinal("019c0000-0000-7000-8000-000000000003", "https://github.com/owner/repo/pull/42", "codex/feature-42")
	if err != nil || !qualified {
		t.Fatalf("qualified/error = %v / %v", qualified, err)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 3 || emitted[0].Params["itemsView"] != "full" || emitted[1].Params["itemsView"] != "notLoaded" || emitted[2].Method != "thread/items/list" {
		t.Fatalf("fallback requests = %#v", emitted)
	}
}

func TestAppServerProtocolRejectsMalformedSearchDataAndRepeatedCursor(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		responses string
	}{
		{name: "missing data", responses: rpcResult(1, `{}`)},
		{name: "repeated cursor", responses: rpcResult(1, searchResult(nil, "same")) + rpcResult(2, searchResult(nil, "same"))},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			client := newAppServerClient(strings.NewReader(testCase.responses), &bytes.Buffer{})
			if err := client.search("term", false, make(map[string]bool)); err == nil {
				t.Fatal("expected protocol error")
			}
		})
	}
}

func TestBoundedReaderStopsAtConfiguredLimit(t *testing.T) {
	t.Parallel()
	reader := &boundedReader{reader: strings.NewReader("abcd"), remaining: 2}
	value, err := io.ReadAll(reader)
	if !errors.Is(err, errAppServerOutputLimit) || string(value) != "ab" {
		t.Fatalf("value/error = %q / %v", value, err)
	}
}

type appServerRequest struct {
	JSONRPC *string        `json:"jsonrpc"`
	ID      int            `json:"id"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params"`
}

type fakeProcessStarter struct {
	request processRequest
	process managedProcess
}

func (starter *fakeProcessStarter) Start(_ context.Context, request processRequest) (managedProcess, error) {
	starter.request = request
	return starter.process, nil
}

type fakeManagedProcess struct {
	input    *writeCloserBuffer
	output   io.ReadCloser
	exit     processExit
	canceled bool
	waited   bool
}

func (process *fakeManagedProcess) Input() io.WriteCloser { return process.input }
func (process *fakeManagedProcess) Output() io.ReadCloser { return process.output }
func (process *fakeManagedProcess) Cancel() {
	process.canceled = true
}
func (process *fakeManagedProcess) Wait() processExit {
	process.waited = true
	return process.exit
}

type writeCloserBuffer struct {
	bytes.Buffer
	closed bool
}

func (buffer *writeCloserBuffer) Close() error {
	buffer.closed = true
	return nil
}

type errorReadCloser struct{ err error }

func (reader errorReadCloser) Read([]byte) (int, error) { return 0, reader.err }
func (errorReadCloser) Close() error                    { return nil }

func assertExactProtocolRequests(t *testing.T, requests []appServerRequest, creatorSessionID string) {
	t.Helper()
	for index, request := range requests {
		if request.JSONRPC != nil {
			t.Fatalf("request %d contains a noncanonical JSON-RPC header", index)
		}
	}
	clientInfo, _ := requests[0].Params["clientInfo"].(map[string]any)
	capabilities, _ := requests[0].Params["capabilities"].(map[string]any)
	if !reflect.DeepEqual(clientInfo, map[string]any{"name": "bifrost", "version": "1"}) || capabilities["experimentalApi"] != true {
		t.Fatalf("initialize request = %#v", requests[0])
	}
	wantSearches := []struct {
		term     string
		archived bool
		cursor   string
	}{
		{term: "https://github.com/owner/repo/pull/42", archived: false},
		{term: "https://github.com/owner/repo/pull/42", archived: false, cursor: "next"},
		{term: "https://github.com/owner/repo/pull/42", archived: true},
	}
	wantSources := make([]any, len(codexTaskSources))
	for index, source := range codexTaskSources {
		wantSources[index] = source
	}
	for index, want := range wantSearches {
		params := requests[index+2].Params
		if params["searchTerm"] != want.term || params["archived"] != want.archived || params["limit"] != float64(threadSearchPageSize) || !reflect.DeepEqual(params["sourceKinds"], wantSources) {
			t.Fatalf("search request %d = %#v", index, params)
		}
		cursor, found := params["cursor"]
		if (want.cursor == "" && found) || (want.cursor != "" && cursor != want.cursor) {
			t.Fatalf("search request %d cursor = %#v", index, cursor)
		}
	}
	turnsParams := requests[5].Params
	if turnsParams["threadId"] != creatorSessionID || turnsParams["limit"] != float64(threadTurnsPageSize) || turnsParams["sortDirection"] != "desc" || turnsParams["itemsView"] != "full" {
		t.Fatalf("turns request = %#v", turnsParams)
	}
}

func decodeAppServerRequests(t *testing.T, encoded []byte) []appServerRequest {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	var requests []appServerRequest
	for {
		var request appServerRequest
		if err := decoder.Decode(&request); errors.Is(err, io.EOF) {
			return requests
		} else if err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
	}
}

func rpcResult(id int, result string) string {
	return fmt.Sprintf(`{"id":%d,"result":%s}`+"\n", id, result)
}

func rpcError(id, code int, message string) string {
	return fmt.Sprintf(`{"id":%d,"error":{"code":%d,"message":%q}}`+"\n", id, code, message)
}

func searchResult(sessionIDs []string, nextCursor string) string {
	items := make([]string, 0, len(sessionIDs))
	for _, sessionID := range sessionIDs {
		items = append(items, fmt.Sprintf(`{"thread":{"id":%q}}`, sessionID))
	}
	cursor := "null"
	if nextCursor != "" {
		cursor = fmt.Sprintf("%q", nextCursor)
	}
	return fmt.Sprintf(`{"data":[%s],"nextCursor":%s}`, strings.Join(items, ","), cursor)
}

func threadTurnsResultJSON(turnID string) string {
	return fmt.Sprintf(`{"data":[{"id":%q,"status":"completed"}],"nextCursor":null}`, turnID)
}

func threadFullTurnsResultJSON(turnID, finalText string, phase *string) string {
	phaseJSON := "null"
	if phase != nil {
		phaseJSON = fmt.Sprintf("%q", *phase)
	}
	return fmt.Sprintf(`{"data":[{"id":%q,"status":"completed","itemsView":"full","items":[{"type":"agentMessage","phase":%s,"text":%q}]}],"nextCursor":null}`, turnID, phaseJSON, finalText)
}

func threadItemsResultJSON(turnID, finalText string, phase *string) string {
	phaseJSON := "null"
	if phase != nil {
		phaseJSON = fmt.Sprintf("%q", *phase)
	}
	return fmt.Sprintf(`{"data":[{"turnId":%q,"item":{"type":"agentMessage","phase":%s,"text":%q}}],"nextCursor":null}`, turnID, phaseJSON, finalText)
}

func stringPointer(value string) *string {
	return &value
}

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
	wantResume := []string{"exec", "--approve-for-me", "resume", "--json", sessionID, "-"}
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

func TestCommandErrorClassifiesAuthenticationWithoutExposingStderr(t *testing.T) {
	t.Parallel()
	err := &commandError{
		cause:  errors.New("exit status 1"),
		stderr: "authentication failed: secret-value",
	}
	message := err.Error()
	if message != "exit status 1: Codex authentication failed" {
		t.Fatalf("error = %q", message)
	}
	if strings.Contains(message, "secret-value") {
		t.Fatalf("error exposed stderr = %q", message)
	}
}

func TestCommandErrorHidesUnclassifiedStderr(t *testing.T) {
	t.Parallel()
	err := &commandError{cause: errors.New("exit status 2"), stderr: "private diagnostic"}
	if message := err.Error(); message != "exit status 2" {
		t.Fatalf("error = %q", message)
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
	runner := &execRunner{environment: environment, processes: osProcessStarter{}}
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
	runner := &execRunner{environment: []string{}, processes: osProcessStarter{}}
	if err := runner.Run(context.Background(), "/usr/bin/env", nil, "", func(line string) {
		lines = append(lines, line)
	}); err != nil {
		t.Fatal(err)
	}
	if len(lines) != 0 {
		t.Fatalf("child inherited environment: %#v", lines)
	}
}

func TestExecRunnerPassesInputThroughManagedProcess(t *testing.T) {
	t.Parallel()
	process := &fakeManagedProcess{
		input:  &writeCloserBuffer{},
		output: io.NopCloser(strings.NewReader("event\n")),
	}
	starter := &fakeProcessStarter{process: process}
	runner := &execRunner{environment: []string{"SAFE=value"}, processes: starter}
	var lines []string

	if err := runner.Run(context.Background(), "codex", []string{"exec"}, "review prompt", func(line string) {
		lines = append(lines, line)
	}); err != nil {
		t.Fatal(err)
	}
	input, err := io.ReadAll(starter.request.Input)
	if err != nil {
		t.Fatal(err)
	}
	if starter.request.Command != "codex" || !reflect.DeepEqual(starter.request.Args, []string{"exec"}) ||
		starter.request.Interactive || string(input) != "review prompt" || !reflect.DeepEqual(lines, []string{"event"}) || !process.waited {
		t.Fatalf("request/lines/waited = %#v / %#v / %t", starter.request, lines, process.waited)
	}
}

func TestExecRunnerCancelsAndWaitsAfterOutputError(t *testing.T) {
	t.Parallel()
	outputError := errors.New("read output")
	process := &fakeManagedProcess{
		input:  &writeCloserBuffer{},
		output: errorReadCloser{err: outputError},
	}
	runner := &execRunner{processes: &fakeProcessStarter{process: process}}

	err := runner.Run(context.Background(), "codex", nil, "", func(string) {})
	if !errors.Is(err, outputError) || !process.canceled || !process.waited {
		t.Fatalf("error/canceled/waited = %v / %t / %t", err, process.canceled, process.waited)
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
