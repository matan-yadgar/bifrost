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

const (
	appServerHelperEnvironment = "GO_WANT_BIFROST_APP_SERVER_HELPER"
	appServerCreatorSessionID  = "019c0000-0000-7000-8000-000000000009"
	appServerForkedSessionID   = "019c0000-0000-7000-8000-000000000010"
)

func TestMain(testingMain *testing.M) {
	if mode := os.Getenv(appServerHelperEnvironment); mode != "" {
		runAppServerHelper(mode)
		return
	}
	os.Exit(testingMain.Run())
}

func TestCodexDiscoveryOwnsAppServerProcess(t *testing.T) {
	target := Target{Repository: "owner/repo", PullRequest: 42, TaskName: "Implement feature"}
	readyPath := filepath.Join(t.TempDir(), "app-server-ready")
	newCodex := func(mode string) *Codex {
		return NewCodex(os.Args[0], nil, []string{
			appServerHelperEnvironment + "=" + mode,
			"BIFROST_APP_SERVER_SENTINEL=expected",
			"BIFROST_APP_SERVER_READY=" + readyPath,
		})
	}

	discoveries, err := newCodex("happy").Discover(context.Background(), []Target{target})
	if err != nil {
		t.Fatal(err)
	}
	if len(discoveries) != 1 || !discoveries[0].Found || discoveries[0].Session.ID != appServerForkedSessionID {
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

}

func TestCodexDiscoveryReconnectsAfterPartialBatchFailure(t *testing.T) {
	t.Parallel()
	firstProcess := &fakeManagedProcess{
		input: &writeCloserBuffer{},
		output: io.NopCloser(strings.NewReader(
			rpcResult(1, `{}`) +
				rpcResult(2, threadListResultJSON([]namedSession{
					{ID: appServerCreatorSessionID, Name: "Implement feature"},
					{ID: "019c0000-0000-7000-8000-000000000008", Name: "Broken task"},
				}, "")) +
				rpcResult(3, threadListResultJSON(nil, "")) +
				rpcResult(4, threadForkResultJSON(appServerForkedSessionID)) +
				rpcResult(5, `{}`),
		)),
		exit: processExit{WaitError: errors.New("app-server exited")},
	}
	secondProcess := &fakeManagedProcess{
		input: &writeCloserBuffer{},
		output: io.NopCloser(strings.NewReader(
			rpcResult(1, `{}`) +
				rpcResult(2, threadListResultJSON([]namedSession{{ID: appServerCreatorSessionID, Name: "Implement feature"}}, "")) +
				rpcResult(3, threadListResultJSON(nil, "")) +
				rpcResult(4, threadForkResultJSON(appServerForkedSessionID)) +
				rpcResult(5, `{}`),
		)),
	}
	starter := &sequenceProcessStarter{processes: []managedProcess{firstProcess, secondProcess}}
	server := &codexAppServer{command: "codex", processes: starter}
	target := Target{Repository: "owner/repo", PullRequest: 42, TaskName: "Implement feature"}
	discoveries, err := server.Discover(context.Background(), []Target{
		target,
		{Repository: "owner/repo", PullRequest: 41, TaskName: "Broken task"},
		target,
	})
	if err != nil || len(discoveries) != 3 || !discoveries[0].Found || discoveries[1].Err == nil || !discoveries[2].Found {
		t.Fatalf("discoveries/error = %#v / %v", discoveries, err)
	}
	if starter.starts != 2 {
		t.Fatalf("app-server starts = %d", starter.starts)
	}
}

func TestCodexDiscoveryUsesManagedProcessTransport(t *testing.T) {
	t.Parallel()
	process := &fakeManagedProcess{
		input: &writeCloserBuffer{},
		output: io.NopCloser(strings.NewReader(
			rpcResult(1, `{}`) +
				rpcResult(2, threadListResultJSON([]namedSession{{ID: appServerCreatorSessionID, Name: "Implement feature"}}, "")) +
				rpcResult(3, threadListResultJSON(nil, "")) +
				rpcResult(4, threadForkResultJSON(appServerForkedSessionID)) +
				rpcResult(5, `{}`),
		)),
	}
	starter := &fakeProcessStarter{process: process}
	server := &codexAppServer{command: "codex", environment: []string{"SAFE=value"}, processes: starter}

	discoveries, err := server.Discover(context.Background(), []Target{{
		Repository: "owner/repo", PullRequest: 42,
		TaskName: "Implement feature",
	}})
	if err != nil || len(discoveries) != 1 || !discoveries[0].Found || discoveries[0].Session.ID != appServerForkedSessionID {
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
	if mode == "hang" {
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}
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
		case "thread/list":
			data := []any{}
			if request.Params["archived"] == true {
				data = append(data, map[string]string{"id": appServerCreatorSessionID, "name": "Implement feature"})
			}
			_ = encoder.Encode(map[string]any{"id": request.ID, "result": map[string]any{"data": data, "nextCursor": nil}})
		case "thread/fork":
			if !reflect.DeepEqual(request.Params, map[string]any{"threadId": appServerCreatorSessionID}) {
				os.Exit(19)
			}
			_ = encoder.Encode(map[string]any{"id": request.ID, "result": map[string]any{
				"thread": map[string]string{"id": appServerForkedSessionID},
			}})
		case "thread/name/set":
			if !reflect.DeepEqual(request.Params, map[string]any{
				"threadId": appServerForkedSessionID,
				"name":     "Bifrost: owner/repo#42",
			}) {
				os.Exit(20)
			}
			_ = encoder.Encode(map[string]any{"id": request.ID, "result": map[string]any{}})
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

func TestAppServerDiscoveryUsesExactTaskNameAcrossPages(t *testing.T) {
	t.Parallel()
	const creatorSessionID = "019c0000-0000-7000-8000-000000000001"
	const partialNameSessionID = "019c0000-0000-7000-8000-000000000002"
	const forkedSessionID = "019c0000-0000-7000-8000-000000000003"
	responses := strings.NewReader(
		rpcResult(1, `{}`) +
			rpcResult(2, threadListResultJSON([]namedSession{{ID: partialNameSessionID, Name: "Implement feature later"}}, "next")) +
			rpcResult(3, threadListResultJSON([]namedSession{{ID: creatorSessionID, Name: "Implement feature"}}, "")) +
			rpcResult(4, threadListResultJSON(nil, "")) +
			rpcResult(5, threadForkResultJSON(forkedSessionID)) +
			rpcResult(6, `{}`),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)

	if err := client.initialize(); err != nil {
		t.Fatal(err)
	}
	discovery := client.discover(Target{Repository: "owner/repo", PullRequest: 42, TaskName: "Implement feature"})
	if discovery.Err != nil {
		t.Fatal(discovery.Err)
	}
	if !discovery.Found || discovery.Session.ID != forkedSessionID {
		t.Fatalf("discovery = %#v", discovery)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 7 {
		t.Fatalf("request count = %d: %#v", len(emitted), emitted)
	}
	wantMethods := []string{"initialize", "initialized", "thread/list", "thread/list", "thread/list", "thread/fork", "thread/name/set"}
	for index, method := range wantMethods {
		if emitted[index].Method != method {
			t.Fatalf("request %d method = %q", index, emitted[index].Method)
		}
	}
	assertExactProtocolRequests(t, emitted)
	if !reflect.DeepEqual(emitted[5].Params, map[string]any{"threadId": creatorSessionID}) {
		t.Fatalf("fork request = %#v", emitted[5])
	}
	if !reflect.DeepEqual(emitted[6].Params, map[string]any{"threadId": forkedSessionID, "name": "Bifrost: owner/repo#42"}) {
		t.Fatalf("name request = %#v", emitted[6])
	}
}

func TestAppServerDiscoveryLoadsTasksOncePerBatch(t *testing.T) {
	t.Parallel()
	const firstCreatorSessionID = "019c0000-0000-7000-8000-000000000001"
	const secondCreatorSessionID = "019c0000-0000-7000-8000-000000000002"
	const firstForkedSessionID = "019c0000-0000-7000-8000-000000000003"
	const secondForkedSessionID = "019c0000-0000-7000-8000-000000000004"
	responses := strings.NewReader(
		rpcResult(1, threadListResultJSON([]namedSession{
			{ID: firstCreatorSessionID, Name: "Implement first feature"},
			{ID: secondCreatorSessionID, Name: "Implement second feature"},
		}, "")) +
			rpcResult(2, threadListResultJSON(nil, "")) +
			rpcResult(3, threadForkResultJSON(firstForkedSessionID)) +
			rpcResult(4, `{}`) +
			rpcResult(5, threadForkResultJSON(secondForkedSessionID)) +
			rpcResult(6, `{}`),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)

	first := client.discover(Target{Repository: "owner/repo", PullRequest: 41, TaskName: "Implement first feature"})
	second := client.discover(Target{Repository: "owner/repo", PullRequest: 42, TaskName: "Implement second feature"})
	if first.Err != nil || !first.Found || first.Session.ID != firstForkedSessionID {
		t.Fatalf("first discovery = %#v", first)
	}
	if second.Err != nil || !second.Found || second.Session.ID != secondForkedSessionID {
		t.Fatalf("second discovery = %#v", second)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 6 {
		t.Fatalf("requests = %#v", emitted)
	}
	wantMethods := []string{"thread/list", "thread/list", "thread/fork", "thread/name/set", "thread/fork", "thread/name/set"}
	for index, method := range wantMethods {
		if emitted[index].Method != method {
			t.Fatalf("request %d method = %q", index, emitted[index].Method)
		}
	}
}

func TestAppServerDiscoveryDoesNotTreatBifrostTitleAsRoute(t *testing.T) {
	t.Parallel()
	const unrelatedSessionID = "019c0000-0000-7000-8000-000000000002"
	responses := strings.NewReader(
		rpcResult(1, threadListResultJSON([]namedSession{{ID: unrelatedSessionID, Name: "Bifrost: owner/repo#42"}}, "")) +
			rpcResult(2, threadListResultJSON(nil, "")),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)
	discovery := client.discover(Target{
		Repository: "owner/repo", PullRequest: 42,
		TaskName: "Implement feature",
	})
	if discovery.Err != nil || discovery.Found || discovery.Session.ID != "" {
		t.Fatalf("discovery = %#v", discovery)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 2 || emitted[0].Method != "thread/list" || emitted[1].Method != "thread/list" {
		t.Fatalf("requests = %#v", emitted)
	}
}

func TestAppServerDiscoveryRequiresExactTaskName(t *testing.T) {
	t.Parallel()
	const namedSessionID = "019c0000-0000-7000-8000-000000000002"
	responses := strings.NewReader(
		rpcResult(1, threadListResultJSON([]namedSession{{ID: namedSessionID, Name: "Implement feature later"}}, "")) +
			rpcResult(2, threadListResultJSON(nil, "")),
	)
	client := newAppServerClient(responses, &bytes.Buffer{})
	discovery := client.discover(Target{
		Repository: "owner/repo", PullRequest: 42,
		TaskName: "Implement feature",
	})
	if discovery.Err != nil || discovery.Found || discovery.Session.ID != "" {
		t.Fatalf("discovery = %#v", discovery)
	}
}

func TestAppServerDiscoveryRejectsInvalidForkResponses(t *testing.T) {
	t.Parallel()
	const creatorSessionID = "019c0000-0000-7000-8000-000000000001"
	prefix := rpcResult(1, threadListResultJSON([]namedSession{{ID: creatorSessionID, Name: "Implement feature"}}, "")) +
		rpcResult(2, threadListResultJSON(nil, ""))
	for _, testCase := range []struct {
		name     string
		response string
	}{
		{name: "RPC error", response: rpcError(3, -32600, "fork failed")},
		{name: "missing thread", response: rpcResult(3, `{}`)},
		{name: "invalid ID", response: rpcResult(3, threadForkResultJSON("invalid"))},
		{name: "creator ID", response: rpcResult(3, threadForkResultJSON(creatorSessionID))},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			var requests bytes.Buffer
			client := newAppServerClient(strings.NewReader(prefix+testCase.response), &requests)
			discovery := client.discover(Target{
				Repository: "owner/repo", PullRequest: 42,
				TaskName: "Implement feature",
			})
			if discovery.Err == nil || discovery.Found || discovery.Session.ID != "" {
				t.Fatalf("discovery = %#v", discovery)
			}
			emitted := decodeAppServerRequests(t, requests.Bytes())
			if len(emitted) != 3 || emitted[2].Method != "thread/fork" {
				t.Fatalf("requests = %#v", emitted)
			}
		})
	}
}

func TestAppServerForkDeletesChildWhenNamingFails(t *testing.T) {
	t.Parallel()
	const forkedSessionID = "019c0000-0000-7000-8000-000000000003"
	responses := strings.NewReader(
		rpcResult(1, threadForkResultJSON(forkedSessionID)) +
			rpcError(2, -32600, "name failed") + rpcResult(3, `{}`),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)
	sessionID, err := client.fork("019c0000-0000-7000-8000-000000000001", "Bifrost: owner/repo#42")
	if err == nil || sessionID != "" {
		t.Fatalf("session/error = %q / %v", sessionID, err)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 3 || emitted[2].Method != "thread/delete" || !reflect.DeepEqual(emitted[2].Params, map[string]any{"threadId": forkedSessionID}) {
		t.Fatalf("requests = %#v", emitted)
	}
}

func TestAppServerDiscoveryRejectsAmbiguousExactTaskNames(t *testing.T) {
	t.Parallel()
	const firstSessionID = "019c0000-0000-7000-8000-000000000001"
	const secondSessionID = "019c0000-0000-7000-8000-000000000002"
	responses := strings.NewReader(
		rpcResult(1, threadListResultJSON([]namedSession{{ID: firstSessionID, Name: "Implement feature"}}, "")) +
			rpcResult(2, threadListResultJSON([]namedSession{{ID: secondSessionID, Name: "Implement feature"}}, "")),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)
	discovery := client.discover(Target{Repository: "owner/repo", PullRequest: 42, TaskName: "Implement feature"})
	if !errors.Is(discovery.Err, ErrAmbiguousSession) {
		t.Fatalf("error = %v", discovery.Err)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 2 || emitted[0].Method != "thread/list" || emitted[1].Method != "thread/list" {
		t.Fatalf("requests = %#v", emitted)
	}
}

func TestAppServerDiscoveryExcludesKnownStaleSessionBeforeAmbiguity(t *testing.T) {
	t.Parallel()
	const olderStaleSessionID = "019c0000-0000-7000-8000-000000000000"
	const staleSessionID = "019c0000-0000-7000-8000-000000000001"
	const replacementSessionID = "019c0000-0000-7000-8000-000000000002"
	const forkedSessionID = "019c0000-0000-7000-8000-000000000003"
	responses := strings.NewReader(
		rpcResult(1, threadListResultJSON([]namedSession{
			{ID: olderStaleSessionID, Name: "Implement feature"},
			{ID: staleSessionID, Name: "Implement feature"},
			{ID: replacementSessionID, Name: "Implement feature"},
		}, "")) +
			rpcResult(2, threadListResultJSON(nil, "")) +
			rpcResult(3, threadForkResultJSON(forkedSessionID)) +
			rpcResult(4, "{}"),
	)
	var requests bytes.Buffer
	client := newAppServerClient(responses, &requests)
	discovery := client.discover(Target{
		Repository: "owner/repo", PullRequest: 42,
		TaskName:           "Implement feature",
		ExcludedSessionIDs: []string{olderStaleSessionID, staleSessionID},
	})
	if discovery.Err != nil || !discovery.Found || discovery.Session.ID != forkedSessionID {
		t.Fatalf("discovery = %#v", discovery)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 4 || emitted[2].Method != "thread/fork" || emitted[2].Params["threadId"] != replacementSessionID || emitted[3].Method != "thread/name/set" {
		t.Fatalf("requests = %#v", emitted)
	}
}

func TestAppServerDiscoveryWithoutTaskNameStartsNewTask(t *testing.T) {
	t.Parallel()
	var requests bytes.Buffer
	client := newAppServerClient(strings.NewReader(""), &requests)
	discovery := client.discover(Target{Repository: "owner/repo", PullRequest: 42})
	if discovery.Err != nil || discovery.Found || discovery.Session.ID != "" {
		t.Fatalf("discovery = %#v", discovery)
	}
	emitted := decodeAppServerRequests(t, requests.Bytes())
	if len(emitted) != 0 {
		t.Fatalf("requests = %#v", emitted)
	}
}

func TestAppServerProtocolRejectsMalformedListDataAndRepeatedCursor(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		responses string
	}{
		{name: "missing data", responses: rpcResult(1, "{}")},
		{name: "repeated cursor", responses: rpcResult(1, threadListResultJSON(nil, "same")) + rpcResult(2, threadListResultJSON(nil, "same"))},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			client := newAppServerClient(strings.NewReader(testCase.responses), &bytes.Buffer{})
			if err := client.listSessions(false, make(map[string]map[string]bool)); err == nil {
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

type sequenceProcessStarter struct {
	processes []managedProcess
	starts    int
}

func (starter *sequenceProcessStarter) Start(_ context.Context, _ processRequest) (managedProcess, error) {
	if starter.starts >= len(starter.processes) {
		return nil, errors.New("unexpected process start")
	}
	process := starter.processes[starter.starts]
	starter.starts++
	return process, nil
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

func assertExactProtocolRequests(t *testing.T, requests []appServerRequest) {
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
	wantLists := []struct {
		archived bool
		cursor   string
	}{
		{archived: false},
		{archived: false, cursor: "next"},
		{archived: true},
	}
	wantSources := make([]any, len(codexTaskSources))
	for index, source := range codexTaskSources {
		wantSources[index] = source
	}
	for index, want := range wantLists {
		params := requests[index+2].Params
		if _, found := params["searchTerm"]; found || params["archived"] != want.archived || params["limit"] != float64(threadListPageSize) || !reflect.DeepEqual(params["sourceKinds"], wantSources) {
			t.Fatalf("list request %d = %#v", index, params)
		}
		cursor, found := params["cursor"]
		if (want.cursor == "" && found) || (want.cursor != "" && cursor != want.cursor) {
			t.Fatalf("list request %d cursor = %#v", index, cursor)
		}
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

type namedSession struct {
	ID   string
	Name string
}

func threadListResultJSON(sessions []namedSession, nextCursor string) string {
	items := make([]string, 0, len(sessions))
	for _, session := range sessions {
		items = append(items, fmt.Sprintf("{\"id\":%q,\"name\":%q}", session.ID, session.Name))
	}
	cursor := "null"
	if nextCursor != "" {
		cursor = fmt.Sprintf("%q", nextCursor)
	}
	return fmt.Sprintf("{\"data\":[%s],\"nextCursor\":%s}", strings.Join(items, ","), cursor)
}

func threadForkResultJSON(sessionID string) string {
	return fmt.Sprintf(`{"thread":{"id":%q}}`, sessionID)
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
