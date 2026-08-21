package harness

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"slices"
	"sync/atomic"
	"time"
)

const (
	maxStderrCaptureBytes = 64 * 1024
	processCleanupTimeout = 5 * time.Second
)

type processRequest struct {
	Command     string
	Args        []string
	Environment []string
	Input       io.Reader
	Interactive bool
}

type processExit struct {
	WaitError           error
	ContextError        error
	CleanupFailed       bool
	PipeCleanupTimedOut bool
	Stderr              string
}

type managedProcess interface {
	Input() io.WriteCloser
	Output() io.ReadCloser
	Cancel()
	Wait() processExit
}

type processStarter interface {
	Start(context.Context, processRequest) (managedProcess, error)
}

type osProcessStarter struct{}

type osManagedProcess struct {
	context             context.Context
	command             *exec.Cmd
	input               io.WriteCloser
	output              io.ReadCloser
	stderr              *boundedBuffer
	cleanupFailed       *atomic.Bool
	pipeCleanupTimedOut *atomic.Bool
	cleanupFinished     chan struct{}
	stopPipeCleanup     func() bool
}

func (osProcessStarter) Start(ctx context.Context, request processRequest) (managedProcess, error) {
	command := exec.CommandContext(ctx, request.Command, request.Args...)
	configureProcess(command)
	cleanupFailed := &atomic.Bool{}
	command.Cancel = func() error {
		err := terminateProcessTree(command.Process)
		if err == nil || errors.Is(err, os.ErrProcessDone) {
			return err
		}
		cleanupFailed.Store(true)
		if rootError := command.Process.Kill(); rootError != nil && !errors.Is(rootError, os.ErrProcessDone) {
			return errors.Join(err, rootError)
		}
		return err
	}
	command.WaitDelay = processCleanupTimeout
	command.Env = slices.Clone(request.Environment)
	stderr := &boundedBuffer{limit: maxStderrCaptureBytes}
	command.Stderr = stderr
	var input io.WriteCloser
	var err error
	if request.Interactive {
		input, err = command.StdinPipe()
		if err != nil {
			return nil, err
		}
	} else {
		command.Stdin = request.Input
	}
	output, err := command.StdoutPipe()
	if err != nil {
		if input != nil {
			_ = input.Close()
		}
		return nil, err
	}
	if err := command.Start(); err != nil {
		if input != nil {
			_ = input.Close()
		}
		_ = output.Close()
		return nil, err
	}
	cleanupFinished := make(chan struct{})
	pipeCleanupTimedOut := &atomic.Bool{}
	stopPipeCleanup := context.AfterFunc(ctx, func() {
		timer := time.NewTimer(processCleanupTimeout)
		defer timer.Stop()
		select {
		case <-cleanupFinished:
		case <-timer.C:
			pipeCleanupTimedOut.Store(true)
			_ = output.Close()
		}
	})
	return &osManagedProcess{
		context: ctx, command: command, input: input, output: output, stderr: stderr,
		cleanupFailed: cleanupFailed, pipeCleanupTimedOut: pipeCleanupTimedOut,
		cleanupFinished: cleanupFinished, stopPipeCleanup: stopPipeCleanup,
	}, nil
}

func (process *osManagedProcess) Input() io.WriteCloser { return process.input }

func (process *osManagedProcess) Output() io.ReadCloser { return process.output }

func (process *osManagedProcess) Cancel() { _ = process.command.Cancel() }

func (process *osManagedProcess) Wait() processExit {
	waitError := process.command.Wait()
	close(process.cleanupFinished)
	process.stopPipeCleanup()
	return processExit{
		WaitError: waitError, ContextError: process.context.Err(), Stderr: process.stderr.String(),
		CleanupFailed: process.cleanupFailed.Load(), PipeCleanupTimedOut: process.pipeCleanupTimedOut.Load(),
	}
}
