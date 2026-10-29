//go:build darwin || linux

package bridge

import (
	"bufio"
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"
)

func TestPythonPauseLockBlocksGoTransactionsAndPreservesConcurrentUpdates(t *testing.T) {
	t.Parallel()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("Python unavailable")
	}
	monitor, _, agent := ciFixture(t)
	if _, err := monitor.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	stale, err := loadState(monitor.stateFile)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, python, "-c", `
import datetime,fcntl,json,os,pathlib,sys,tempfile
path=pathlib.Path(sys.argv[1])
with open(str(path)+'.update.lock','a+b') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX)
 print('locked',flush=True)
 sys.stdin.readline()
 state=json.loads(path.read_text())
 entry=state['ci']['owner/repo#42']
 entry.update(paused=True,paused_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),reason='human credentials needed',resume_condition='credentials restored')
 fd,name=tempfile.mkstemp(dir=path.parent)
 with os.fdopen(fd,'w') as output:
  json.dump(state,output)
  output.flush()
  os.fsync(output.fileno())
 os.replace(name,path)
`, monitor.stateFile)
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close(); cancel(); _ = command.Wait() }()
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil || line != "locked\n" {
		t.Fatalf("lock readiness=%q err=%v", line, err)
	}
	blocked, cancelBlocked := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, err = monitor.readState(blocked)
	cancelBlocked()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Go bypassed Python's lock: %v", err)
	}
	if _, err := input.Write([]byte("release\n")); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := monitor.saveState(ctx, stale); err != nil {
		t.Fatal(err)
	}
	state, err := monitor.readState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	saved := state.CI["owner/repo#42"]
	if !saved.Paused || saved.Reason != "human credentials needed" || state.Routes["owner/repo#42"].SessionID != agent.discoveredID {
		t.Fatalf("lost state: %+v routes=%+v", saved, state.Routes)
	}
}
