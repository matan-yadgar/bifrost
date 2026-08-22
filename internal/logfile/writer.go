package logfile

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	maxLinesPerFile = 1000
	filenameLayout  = "2006-01-02_15-04-05"
)

type writer struct {
	mutex     sync.Mutex
	directory string
	now       func() time.Time
	file      *os.File
	lineCount int
}

type output struct {
	terminal       io.Writer
	persistent     io.Writer
	warnPersistent sync.Once
}

func New(directory string) (io.WriteCloser, error) {
	return newWriter(directory, time.Now)
}

func NewOutput(terminal, persistent io.Writer) io.Writer {
	return &output{terminal: terminal, persistent: persistent}
}

func newWriter(directory string, now func() time.Time) (*writer, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}
	writer := &writer{directory: directory, now: now}
	if _, err := writer.rotate(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (writer *writer) Write(data []byte) (int, error) {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	if writer.file == nil {
		return 0, errors.New("log writer is closed")
	}
	written := 0
	var rotationErrors []error
	for len(data) > 0 {
		if writer.lineCount == maxLinesPerFile {
			rotated, err := writer.rotate()
			if err != nil {
				if !rotated {
					return written, errors.Join(append(rotationErrors, err)...)
				}
				rotationErrors = append(rotationErrors, err)
			}
		}
		chunkLength := len(data)
		if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
			chunkLength = newline + 1
		}
		count, err := writer.file.Write(data[:chunkLength])
		written += count
		if err != nil {
			return written, errors.Join(append(rotationErrors, err)...)
		}
		if count != chunkLength {
			return written, errors.Join(append(rotationErrors, io.ErrShortWrite)...)
		}
		if data[chunkLength-1] == '\n' {
			writer.lineCount++
		}
		data = data[chunkLength:]
	}
	return written, errors.Join(rotationErrors...)
}

func (output *output) Write(data []byte) (int, error) {
	terminalCount, terminalError := output.terminal.Write(data)
	if terminalError == nil && terminalCount != len(data) {
		terminalError = io.ErrShortWrite
	}
	persistentCount, persistentError := output.persistent.Write(data)
	if persistentError == nil && persistentCount != len(data) {
		persistentError = io.ErrShortWrite
	}
	if persistentError != nil {
		output.warnPersistent.Do(func() {
			_, _ = fmt.Fprintf(output.terminal, "bifrost persistent logging failed: %v\n", persistentError)
		})
	}
	return min(terminalCount, persistentCount), errors.Join(terminalError, persistentError)
}

func (writer *writer) Close() error {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	if writer.file == nil {
		return nil
	}
	err := writer.file.Close()
	writer.file = nil
	return err
}

func (writer *writer) rotate() (bool, error) {
	file, err := writer.createFile()
	if err != nil {
		return false, err
	}
	previous := writer.file
	writer.file = file
	writer.lineCount = 0
	if previous != nil {
		return true, previous.Close()
	}
	return true, nil
}

func (writer *writer) createFile() (*os.File, error) {
	baseName := "bifrost_" + writer.now().Format(filenameLayout)
	for suffix := 0; ; suffix++ {
		name := baseName
		if suffix > 0 {
			name += "_" + strconv.Itoa(suffix)
		}
		path := filepath.Join(writer.directory, name+".log")
		file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create log file: %w", err)
		}
	}
}
