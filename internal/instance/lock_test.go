package instance

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestAcquireRejectsSecondLockWhileHeld(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	first, err := Acquire(stateFile)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := Acquire(stateFile)
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second acquisition error = %v, want ErrAlreadyRunning", err)
	}
}

func TestAcquireSucceedsAfterRelease(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	first, err := Acquire(stateFile)
	if err != nil {
		t.Fatal(err)
	}

	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(stateFile)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireAllowsDistinctStateFiles(t *testing.T) {
	directory := t.TempDir()
	first, err := Acquire(filepath.Join(directory, "first.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Close() }()

	second, err := Acquire(filepath.Join(directory, "second.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}
