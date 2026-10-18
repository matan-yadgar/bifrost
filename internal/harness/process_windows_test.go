//go:build windows

package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestExecRunnerLaunchReturnsBeforeChildCompletes(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "completed")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &execRunner{processes: osProcessStarter{}}
	command := fmt.Sprintf(`echo started & timeout /t 1 /nobreak > nul & echo done > "%s"`, markerPath)
	err := runner.Launch(ctx, "cmd", []string{"/c", command}, "", func(line string) bool {
		return line == "started"
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(markerPath); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("launched process did not survive context cancellation")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExecRunnerLaunchCancelsUnacknowledgedProcess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &execRunner{processes: osProcessStarter{}}
	completed := make(chan error, 1)
	started := make(chan struct{}, 1)
	var startedOnce sync.Once
	go func() {
		completed <- runner.Launch(ctx, "cmd", []string{"/c", "echo ready & timeout /t 30 /nobreak > nul"}, "", func(line string) bool {
			if line == "ready" {
				startedOnce.Do(func() { started <- struct{}{} })
			}
			return false
		})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("launch did not start")
	}
	cancel()
	select {
	case err := <-completed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled launch did not return")
	}
}
