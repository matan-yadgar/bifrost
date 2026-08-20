//go:build darwin || linux

package harness

import (
	"os"
	"os/exec"
	"syscall"
)

func configureProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func terminateProcessTree(process *os.Process) error {
	if err := syscall.Kill(-process.Pid, syscall.SIGKILL); err == syscall.ESRCH {
		return os.ErrProcessDone
	} else {
		return err
	}
}
