package instance

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/gofrs/flock"
)

var ErrAlreadyRunning = errors.New("another bifrost process is already using this state file")

type Lock struct {
	file *flock.Flock
}

func Acquire(stateFile string) (*Lock, error) {
	stateDirectory := filepath.Dir(stateFile)
	if err := os.MkdirAll(stateDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	stateDirectory, err := filepath.EvalSymlinks(stateDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve state directory: %w", err)
	}

	lockPath := filepath.Join(stateDirectory, filepath.Base(stateFile)) + ".lock"
	file := flock.New(lockPath, flock.SetPermissions(0o600))
	locked, err := file.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock state file %q: %w", stateFile, err)
	}
	if !locked {
		return nil, fmt.Errorf("%w: %s", ErrAlreadyRunning, stateFile)
	}
	return &Lock{file: file}, nil
}

func (lock *Lock) Close() error {
	return lock.file.Close()
}
