package instance

import (
	"errors"
	"os"
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

func TestAcquireRejectsSymlinkedDirectoryAlias(t *testing.T) {
	directory := t.TempDir()
	realDirectory := filepath.Join(directory, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	aliasDirectory := filepath.Join(directory, "alias")
	if err := os.Symlink(realDirectory, aliasDirectory); err != nil {
		t.Skipf("directory symlinks are unavailable: %v", err)
	}

	first, err := Acquire(filepath.Join(realDirectory, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := Acquire(filepath.Join(aliasDirectory, "state.json"))
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("alias acquisition error = %v, want ErrAlreadyRunning", err)
	}
}
