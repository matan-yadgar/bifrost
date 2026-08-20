//go:build windows

package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

func configureProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

func terminateProcessTree(process *os.Process) error {
	cleanupContext, cancel := context.WithTimeout(context.Background(), processCleanupTimeout)
	defer cancel()
	treeError := exec.CommandContext(cleanupContext, "taskkill", "/T", "/F", "/PID", strconv.Itoa(process.Pid)).Run()
	if treeError == nil {
		return nil
	}
	rootError := process.Kill()
	if errors.Is(rootError, os.ErrProcessDone) {
		return fmt.Errorf("descendant cleanup could not be verified: %w", treeError)
	}
	if rootError != nil {
		return errors.Join(treeError, rootError)
	}
	return fmt.Errorf("descendant cleanup failed after terminating root: %w", treeError)
}
