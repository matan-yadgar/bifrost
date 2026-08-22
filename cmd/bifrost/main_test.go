package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
