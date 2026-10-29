//go:build darwin || linux

package harness

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExecRunnerTerminatesChildProcessTree(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &execRunner{processes: osProcessStarter{}}
	childPIDs := make(chan int, 1)
	completed := make(chan error, 1)
	go func() {
		completed <- runner.Run(ctx, "sh", []string{"-c", "sleep 30 & child=$!; echo $child; wait"}, "", func(line string) {
			childPID, _ := strconv.Atoi(strings.TrimSpace(line))
			childPIDs <- childPID
		})
	}()
	var childPID int
	select {
	case childPID = <-childPIDs:
	case <-time.After(2 * time.Second):
		t.Fatal("child PID was not reported")
	}
	if childPID == 0 {
		t.Fatal("reported child PID was invalid")
	}
	cancel()
	var err error
	select {
	case err = <-completed:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled runner did not return")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		output, processError := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(childPID)).Output()
		state := strings.TrimSpace(string(output))
		if processError != nil || strings.HasPrefix(state, "Z") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("child process %d survived with state %q", childPID, state)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestExecRunnerLaunchReturnsBeforeChildCompletes(t *testing.T) {
	t.Parallel()
	markerPath := filepath.Join(t.TempDir(), "completed")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &execRunner{processes: osProcessStarter{}}
	err := runner.Launch(ctx, "sh", []string{"-c", "printf 'started\\n'; sleep 1; : > \"$1\"", "sh", markerPath}, "", func(line string) bool {
		return line == "started"
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	deadline := time.Now().Add(2 * time.Second)
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
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := &execRunner{processes: osProcessStarter{}}
	completed := make(chan error, 1)
	started := make(chan struct{}, 1)
	var startedOnce sync.Once
	go func() {
		completed <- runner.Launch(ctx, "sh", []string{"-c", "printf 'ready\\n'; sleep 30"}, "", func(line string) bool {
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

func TestExecRunnerLaunchReturnsOnlyStderrOnFailure(t *testing.T) {
	t.Parallel()
	runner := &execRunner{processes: osProcessStarter{}}
	err := runner.Launch(context.Background(), "sh", []string{"-c", "printf stdout; printf 'launch failure\\n' >&2; exit 7"}, "", func(string) bool {
		return false
	})
	if diagnostic := Diagnostic(err); diagnostic != "launch failure" {
		t.Fatalf("diagnostic = %q", diagnostic)
	}
}
