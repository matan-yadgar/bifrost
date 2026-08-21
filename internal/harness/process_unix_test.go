//go:build darwin || linux

package harness

import (
	"context"
	"errors"
	"os/exec"
	"strconv"
	"strings"
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
