package logfile

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestWriterRotatesAndStartsWithANewFile(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "missing", "logs")
	fixedTime := time.Date(2026, 8, 22, 14, 30, 45, 0, time.Local)
	newTestWriter := func() *writer {
		writer, err := newWriter(directory, func() time.Time { return fixedTime })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = writer.Close() })
		return writer
	}

	writer := newTestWriter()
	payload := []byte(strings.Repeat("line\n", maxLinesPerFile) + "line 1001\n")
	if written, err := writer.Write(payload); err != nil || written != len(payload) {
		t.Fatalf("write = %d / %v", written, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	restartedWriter := newTestWriter()
	if err := restartedWriter.Close(); err != nil {
		t.Fatal(err)
	}
	files := logFiles(t, directory)
	expected := []string{
		"bifrost_2026-08-22_14-30-45.log",
		"bifrost_2026-08-22_14-30-45_1.log",
		"bifrost_2026-08-22_14-30-45_2.log",
	}
	if !slices.Equal(files, expected) {
		t.Fatalf("files = %#v", files)
	}
	if lineCount(t, filepath.Join(directory, files[0])) != maxLinesPerFile {
		t.Fatalf("first file did not contain %d lines", maxLinesPerFile)
	}
	if lineCount(t, filepath.Join(directory, files[1])) != 1 {
		t.Fatal("rotated file did not contain line 1001")
	}
	if lineCount(t, filepath.Join(directory, files[2])) != 0 {
		t.Fatal("restart file was not new and empty")
	}
	first, err := os.ReadFile(filepath.Join(directory, files[0]))
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(filepath.Join(directory, files[1]))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(append(first, second...), payload) {
		t.Fatal("rotation did not preserve the payload")
	}
}

func TestNewRejectsARegularFileAsTheDirectory(t *testing.T) {
	t.Parallel()
	directory := filepath.Join(t.TempDir(), "logs")
	if err := os.WriteFile(directory, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(directory); err == nil {
		t.Fatal("expected directory creation to fail")
	}
	data, err := os.ReadFile(directory)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "existing" {
		t.Fatal("existing file was changed")
	}
}

func TestOutputReportsPersistentFailureAndStillUsesBothSinks(t *testing.T) {
	t.Parallel()
	t.Run("persistent failure", func(t *testing.T) {
		var terminal bytes.Buffer
		failure := errors.New("disk full")
		output := NewOutput(&terminal, failingWriter{err: failure})
		if _, err := output.Write([]byte("second\n")); !errors.Is(err, failure) {
			t.Fatalf("write error = %v", err)
		}
		if _, err := output.Write([]byte("third\n")); !errors.Is(err, failure) {
			t.Fatalf("second write error = %v", err)
		}
		want := "second\nbifrost persistent logging failed: disk full\nthird\n"
		if terminal.String() != want {
			t.Fatalf("terminal output = %q", terminal.String())
		}
	})
	t.Run("terminal failure", func(t *testing.T) {
		var persistent bytes.Buffer
		failure := errors.New("terminal closed")
		output := NewOutput(failingWriter{err: failure}, &persistent)
		if _, err := output.Write([]byte("saved\n")); !errors.Is(err, failure) {
			t.Fatalf("write error = %v", err)
		}
		if persistent.String() != "saved\n" {
			t.Fatalf("persistent output = %q", persistent.String())
		}
	})
}

type failingWriter struct{ err error }

func (writer failingWriter) Write([]byte) (int, error) { return 0, writer.err }

var _ io.Writer = failingWriter{}

func logFiles(t *testing.T, directory string) []string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		files = append(files, entry.Name())
	}
	return files
}

func lineCount(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Count(data, []byte{'\n'})
}
