package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/matan-yadgar/bifrost/internal/bridge"
)

func TestNewPersistentLoggerMirrorsOutput(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "missing", "logs")
	var terminal bytes.Buffer
	logger, closer, err := newPersistentLogger(directory, &terminal)
	if err != nil {
		t.Fatal(err)
	}
	logger.Print("connected")
	if err := closer.Close(); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("log files = %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(directory, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(terminal.String(), "connected") || !bytes.Contains(data, []byte("connected")) {
		t.Fatalf("terminal = %q, file = %q", terminal.String(), data)
	}
}

func TestControlUIShowsConfigLogsAndControlsPoller(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.json")
	configContents := []byte(`{"poll_interval":"5m"}`)
	if err := os.WriteFile(configPath, configContents, 0o600); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	stopped := make(chan struct{})
	logs := newLiveLogs()
	ui := &controlUI{
		configPath: configPath,
		logger:     log.New(io.Discard, "", 0),
		token:      "test-token",
		logs:       logs,
		run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			close(stopped)
			return nil
		},
	}
	server := httptest.NewServer(ui.handler())
	defer server.Close()

	if _, err := logs.Write([]byte("first event\n")); err != nil {
		t.Fatal(err)
	}
	response, err := http.Get(server.URL + "/api/config")
	if err != nil {
		t.Fatal(err)
	}
	configResponse, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(configResponse) != string(configContents) {
		t.Fatalf("config response = status %d, body %q", response.StatusCode, configResponse)
	}

	response, err = http.Get(server.URL + "/api/logs")
	if err != nil {
		t.Fatal(err)
	}
	var logResponse struct {
		Lines []string `json:"lines"`
	}
	if err := json.NewDecoder(response.Body).Decode(&logResponse); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if len(logResponse.Lines) != 1 || logResponse.Lines[0] != "first event\n" {
		t.Fatalf("log response = %#v", logResponse)
	}

	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/activate", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unauthorized status = %d", response.StatusCode)
	}

	request, err = http.NewRequest(http.MethodPost, server.URL+"/api/activate", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Bifrost-Token", ui.token)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("activate status = %d", response.StatusCode)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("poller did not start")
	}
	if !ui.status().Active {
		t.Fatal("UI reports inactive after activation")
	}

	request, err = http.NewRequest(http.MethodPost, server.URL+"/api/deactivate", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Bifrost-Token", ui.token)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("deactivate status = %d", response.StatusCode)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("poller did not stop")
	}
	<-ui.finished()
	if ui.status().Active {
		t.Fatal("UI reports active after deactivation")
	}
}

func TestIncompleteDeliveryError(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name   string
		result bridge.CycleResult
		want   string
	}{
		{name: "complete"},
		{name: "pull requests deferred", result: bridge.CycleResult{Deferred: 2}, want: "2 pull requests"},
		{name: "threads deferred", result: bridge.CycleResult{DeferredThreads: 3}, want: "3 review threads"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := incompleteDeliveryError(testCase.result)
			if testCase.want == "" && err != nil {
				t.Fatal(err)
			}
			if testCase.want != "" && (err == nil || !strings.Contains(err.Error(), testCase.want)) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
