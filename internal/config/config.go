package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultPollInterval    = 5 * time.Minute
	defaultDispatchTimeout = 30 * time.Minute
)

type Config struct {
	PollInterval    string       `json:"poll_interval"`
	DispatchTimeout string       `json:"dispatch_timeout"`
	StateFile       string       `json:"state_file"`
	LogDirectory    string       `json:"log_directory,omitempty"`
	Repositories    []Repository `json:"repositories"`
	Harness         Harness      `json:"harness"`
}

type Repository struct {
	Name             string   `json:"name"`
	Authors          []string `json:"authors,omitempty"`
	WorkingDirectory string   `json:"working_directory"`
}

type Harness struct {
	Type    string   `json:"type"`
	Command string   `json:"command,omitempty"`
	Args    []string `json:"args,omitempty"`
}

type Runtime struct {
	PollInterval    time.Duration
	DispatchTimeout time.Duration
	StateFile       string
	LogDirectory    string
	Repositories    []Repository
	Harness         Harness
}

func DefaultPath() (string, error) {
	directory, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(directory, "bifrost", "config.json"), nil
}

func Load(path string) (Runtime, error) {
	path, err := expandPath(path)
	if err != nil {
		return Runtime{}, err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return Runtime{}, fmt.Errorf("resolve config path: %w", err)
	}
	file, err := os.Open(path)
	if err != nil {
		return Runtime{}, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Runtime{}, fmt.Errorf("decode config: %w", err)
	}

	baseDirectory := filepath.Dir(path)
	pollInterval, err := positiveDuration("poll_interval", config.PollInterval, defaultPollInterval)
	if err != nil {
		return Runtime{}, err
	}
	dispatchTimeout, err := positiveDuration("dispatch_timeout", config.DispatchTimeout, defaultDispatchTimeout)
	if err != nil {
		return Runtime{}, err
	}
	if len(config.Repositories) == 0 {
		return Runtime{}, fmt.Errorf("at least one repository is required")
	}
	seenRepositories := make(map[string]bool, len(config.Repositories))
	for index := range config.Repositories {
		repository := &config.Repositories[index]
		repository.Name = strings.TrimSpace(repository.Name)
		if !validRepository(repository.Name) {
			return Runtime{}, fmt.Errorf("repositories[%d].name must be owner/repo", index)
		}
		repositoryKey := strings.ToLower(repository.Name)
		if seenRepositories[repositoryKey] {
			return Runtime{}, fmt.Errorf("repositories[%d].name duplicates %s", index, repository.Name)
		}
		seenRepositories[repositoryKey] = true
		repository.Authors = normalizeAuthors(repository.Authors)
		repository.WorkingDirectory, err = resolvePath(baseDirectory, repository.WorkingDirectory)
		if err != nil {
			return Runtime{}, fmt.Errorf("repositories[%d].working_directory: %w", index, err)
		}
		info, statErr := os.Stat(repository.WorkingDirectory)
		if statErr != nil {
			return Runtime{}, fmt.Errorf("repositories[%d].working_directory: %w", index, statErr)
		}
		if !info.IsDir() {
			return Runtime{}, fmt.Errorf("repositories[%d].working_directory is not a directory", index)
		}
		repository.WorkingDirectory, err = filepath.EvalSymlinks(repository.WorkingDirectory)
		if err != nil {
			return Runtime{}, fmt.Errorf("repositories[%d].working_directory: %w", index, err)
		}
	}

	config.Harness.Type = strings.ToLower(strings.TrimSpace(config.Harness.Type))
	if config.Harness.Type == "" {
		config.Harness.Type = "codex"
	}
	if strings.TrimSpace(config.Harness.Command) == "" {
		config.Harness.Command = "codex"
	}

	stateFile := config.StateFile
	if strings.TrimSpace(stateFile) == "" {
		stateFile = "state.json"
	}
	stateFile, err = resolvePath(baseDirectory, stateFile)
	if err != nil {
		return Runtime{}, fmt.Errorf("state_file: %w", err)
	}
	if pathsOverlap(stateFile, path) {
		return Runtime{}, fmt.Errorf("config and state_file must use different paths")
	}
	logDirectory := config.LogDirectory
	if strings.TrimSpace(logDirectory) == "" {
		logDirectory = "logs"
	}
	logDirectory, err = resolvePath(baseDirectory, logDirectory)
	if err != nil {
		return Runtime{}, fmt.Errorf("log_directory: %w", err)
	}
	if pathContains(stateFile, logDirectory) || pathContains(path, logDirectory) {
		return Runtime{}, fmt.Errorf("log_directory must differ from config and state_file paths")
	}
	return Runtime{
		PollInterval:    pollInterval,
		DispatchTimeout: dispatchTimeout,
		StateFile:       stateFile,
		LogDirectory:    logDirectory,
		Repositories:    config.Repositories,
		Harness:         config.Harness,
	}, nil
}

func positiveDuration(name, value string, defaultValue time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return defaultValue, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", name)
	}
	return duration, nil
}

func pathsOverlap(left, right string) bool {
	return pathContains(left, right) || pathContains(right, left)
}

func pathContains(directory, path string) bool {
	relative, err := filepath.Rel(directory, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func validRepository(value string) bool {
	parts := strings.Split(value, "/")
	return len(parts) == 2 && validRepositorySegment(parts[0]) && validRepositorySegment(parts[1])
}

func validRepositorySegment(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, `\#`)
}

func normalizeAuthors(authors []string) []string {
	seen := make(map[string]bool)
	normalized := make([]string, 0, len(authors))
	for _, author := range authors {
		author = strings.TrimSpace(strings.TrimPrefix(author, "@"))
		key := strings.ToLower(author)
		if author == "" || seen[key] {
			continue
		}
		seen[key] = true
		normalized = append(normalized, author)
	}
	return normalized
}

func resolvePath(baseDirectory, path string) (string, error) {
	path, err := expandPath(strings.TrimSpace(path))
	if err != nil {
		return "", err
	}
	if path == "" {
		return "", fmt.Errorf("path is required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDirectory, path)
	}
	return filepath.Clean(path), nil
}

func expandPath(path string) (string, error) {
	if path == "~" || strings.HasPrefix(path, "~/") || strings.HasPrefix(path, `~\`) {
		homeDirectory, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if path == "~" {
			return homeDirectory, nil
		}
		return filepath.Join(homeDirectory, path[2:]), nil
	}
	return path, nil
}
