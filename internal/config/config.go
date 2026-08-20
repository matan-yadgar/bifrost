package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const defaultPollInterval = 5 * time.Minute

type Config struct {
	PollInterval string       `json:"poll_interval"`
	StateFile    string       `json:"state_file"`
	MappingFile  string       `json:"mapping_file"`
	Repositories []Repository `json:"repositories"`
	Harness      Harness      `json:"harness"`
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
	PollInterval time.Duration
	StateFile    string
	MappingFile  string
	Repositories []Repository
	Harness      Harness
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
	pollInterval := defaultPollInterval
	if strings.TrimSpace(config.PollInterval) != "" {
		pollInterval, err = time.ParseDuration(config.PollInterval)
		if err != nil || pollInterval <= 0 {
			return Runtime{}, fmt.Errorf("poll_interval must be a positive Go duration")
		}
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
	mappingFile := config.MappingFile
	if strings.TrimSpace(mappingFile) == "" {
		mappingFile = "mappings.json"
	}
	mappingFile, err = resolvePath(baseDirectory, mappingFile)
	if err != nil {
		return Runtime{}, fmt.Errorf("mapping_file: %w", err)
	}
	if stateFile == mappingFile || stateFile == path || mappingFile == path {
		return Runtime{}, fmt.Errorf("config, state_file, and mapping_file must use different paths")
	}

	return Runtime{
		PollInterval: pollInterval,
		StateFile:    stateFile,
		MappingFile:  mappingFile,
		Repositories: config.Repositories,
		Harness:      config.Harness,
	}, nil
}

func validRepository(value string) bool {
	parts := strings.Split(value, "/")
	return len(parts) == 2 && parts[0] != "" && parts[1] != ""
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
