package bridge

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

func (monitor *Monitor) stateLock(ctx context.Context) (*flock.Flock, error) {
	// The process-lifetime .lock remains separate from short state transactions.
	if err := os.MkdirAll(filepath.Dir(monitor.stateFile), 0o700); err != nil {
		return nil, err
	}
	lock := flock.New(monitor.stateFile+".update.lock", flock.SetPermissions(0o600))
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil || !locked {
		_ = lock.Close()
		if err == nil {
			err = ctx.Err()
		}
		return nil, fmt.Errorf("lock PR state: %w", err)
	}
	return lock, nil
}

func (monitor *Monitor) readState(ctx context.Context) (state *stateFile, err error) {
	lock, err := monitor.stateLock(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	return loadState(monitor.stateFile)
}

func (monitor *Monitor) saveState(ctx context.Context, state *stateFile) (err error) {
	lock, err := monitor.stateLock(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, lock.Close()) }()
	latest, err := loadState(monitor.stateFile)
	if err != nil {
		return err
	}
	for key, entry := range state.CI {
		if current := latest.CI[key]; current != nil {
			entry.Paused, entry.Reason, entry.ResumeCondition = current.Paused, current.Reason, current.ResumeCondition
		}
	}
	return saveJSON(monitor.stateFile, state)
}
