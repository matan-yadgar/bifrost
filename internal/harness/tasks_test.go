package harness

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadTaskResolvesExactNameWithoutForkingOrResuming(t *testing.T) {
	t.Parallel()
	input := &writeCloserBuffer{}
	process := &fakeManagedProcess{input: input, output: io.NopCloser(strings.NewReader(
		rpcResult(1, `{}`) +
			rpcResult(2, threadListResultJSON([]namedSession{{ID: appServerCreatorSessionID, Name: "Feature"}}, "")) +
			rpcResult(3, threadListResultJSON(nil, "")) +
			rpcResult(4, `{"thread":{"id":"`+appServerCreatorSessionID+`","turns":[{"status":"completed"}]}}`),
	))}
	server := &codexAppServer{command: "codex", processes: &fakeProcessStarter{process: process}}
	history, err := server.ReadTask(context.Background(), "Feature")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(history), appServerCreatorSessionID) {
		t.Fatal("history not returned")
	}
	requests := decodeAppServerRequests(t, input.Bytes())
	if len(requests) != 5 || requests[4].Method != "thread/read" || requests[4].Params["threadId"] != appServerCreatorSessionID {
		t.Fatalf("requests=%#v", requests)
	}
	for _, request := range requests {
		if request.Method == "thread/fork" || request.Method == "thread/resume" {
			t.Fatal("read took ownership")
		}
	}
	if !process.waited || !input.closed {
		t.Fatal("app-server not cleaned up")
	}
}

func TestReadTaskRejectsAmbiguousNames(t *testing.T) {
	t.Parallel()
	input := &writeCloserBuffer{}
	process := &fakeManagedProcess{input: input, output: io.NopCloser(strings.NewReader(
		rpcResult(1, `{}`) + rpcResult(2, threadListResultJSON([]namedSession{{ID: appServerCreatorSessionID, Name: "Feature"}, {ID: appServerForkedSessionID, Name: "Feature"}}, "")) + rpcResult(3, threadListResultJSON(nil, "")),
	))}
	server := &codexAppServer{command: "codex", processes: &fakeProcessStarter{process: process}}
	if _, err := server.ReadTask(context.Background(), "Feature"); err == nil {
		t.Fatal("ambiguous task accepted")
	}
	for _, request := range decodeAppServerRequests(t, input.Bytes()) {
		if request.Method == "thread/read" {
			t.Fatal("ambiguous task read")
		}
	}
}

func TestWakeTaskQueuesOnlyForAnExistingWriter(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, diagnostic string
		queued           bool
	}{
		{"Desktop owns thread", "thread-store conflict: already has an active writer", true},
		{"auth failure", "authentication failed", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			process := &fakeManagedProcess{output: io.NopCloser(strings.NewReader("Queued message receipt for thread " + appServerCreatorSessionID))}
			starter := &fakeProcessStarter{process: process}
			codex := NewCodex("codex", []string{"--skip-git-repo-check"}, nil)
			codex.runner = &fakeRunner{err: &commandError{cause: errors.New("exit status 1"), stderr: test.diagnostic}}
			codex.appServer.processes = starter
			delivery, err := codex.WakeTask(context.Background(), appServerCreatorSessionID, "check current CI")
			if test.queued {
				if strings.Contains(strings.Join(starter.request.Args, " "), "--skip-git-repo-check") {
					t.Fatal("exec flags leaked into queue")
				}
				if err != nil || delivery != "queued" || starter.request.Command != "codex" {
					t.Fatalf("delivery=%s error=%v request=%#v", delivery, err, starter.request)
				}
			} else if err == nil || starter.request.Command != "" {
				t.Fatal("unrelated failure queued a duplicate message")
			}
		})
	}
}

func TestWakeTaskResumesAnUnloadedThread(t *testing.T) {
	t.Parallel()
	codex := NewCodex("codex", nil, nil)
	codex.runner = &fakeRunner{lines: []string{`{"type":"thread.started","thread_id":"` + appServerCreatorSessionID + `"}`}}
	delivery, err := codex.WakeTask(context.Background(), appServerCreatorSessionID, "check CI")
	if err != nil || delivery != "resumed" {
		t.Fatalf("delivery=%s error=%v", delivery, err)
	}
}
