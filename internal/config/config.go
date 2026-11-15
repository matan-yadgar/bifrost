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
	MonitorCI       bool         `json:"monitor_ci,omitempty"`
	PollInterval    string       `json:"poll_interval"`
	DispatchTimeout string       `json:"dispatch_timeout"`
	StateFile       string       `json:"state_file"`
	LogDirectory    string       `json:"log_directory,omitempty"`
	PRManager       PRManager    `json:"pr_manager,omitempty"`
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

type PRManager struct {
	Enabled      bool   `json:"enabled,omitempty"`
	AutoReply    bool   `json:"auto_reply,omitempty"`
	UserTokenEnv string `json:"user_token_env,omitempty"`
	Conversation string `json:"conversation_id,omitempty"`
	BotID        string `json:"bot_id,omitempty"`
}

type Runtime struct {
	MonitorCI       bool
	PollInterval    time.Duration
	DispatchTimeout time.Duration
	StateFile       string
	LogDirectory    string
	PRManager       PRManager
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
		if config.MonitorCI && len(repository.Authors) == 0 {
			return Runtime{}, fmt.Errorf("repositories[%d].authors is required when monitor_ci is enabled", index)
		}
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
	config.PRManager, err = normalizePRManager(config.PRManager)
	if err != nil {
		return Runtime{}, err
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
	return Runtime{
		MonitorCI:       config.MonitorCI,
		PollInterval:    pollInterval,
		DispatchTimeout: dispatchTimeout,
		StateFile:       stateFile,
		LogDirectory:    logDirectory,
		PRManager:       config.PRManager,
		Repositories:    config.Repositories,
		Harness:         config.Harness,
	}, nil
}

// UpdatePRManagerAutoReply persists the PR Manager reply mode selected in the
// local control UI.
func UpdatePRManagerAutoReply(path string, autoReply bool) error {
	path, err := expandPath(path)
	if err != nil {
		return err
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve config path: %w", err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(contents, &document); err != nil {
		return fmt.Errorf("decode config: %w", err)
	}
	if document == nil {
		return fmt.Errorf("config must be an object")
	}
	prManager := make(map[string]json.RawMessage)
	if encoded, found := document["pr_manager"]; found {
		if err := json.Unmarshal(encoded, &prManager); err != nil {
			return fmt.Errorf("decode pr_manager: %w", err)
		}
		if prManager == nil {
			return fmt.Errorf("pr_manager must be an object")
		}
	}
	autoReplyJSON, err := json.Marshal(autoReply)
	if err != nil {
		return fmt.Errorf("encode auto_reply: %w", err)
	}
	prManager["auto_reply"] = autoReplyJSON
	document["pr_manager"], err = json.Marshal(prManager)
	if err != nil {
		return fmt.Errorf("encode pr_manager: %w", err)
	}
	contents, err = json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("encode config: %w", err)
	}
	contents = append(contents, '\n')
	temporary, err := os.CreateTemp(filepath.Dir(path), ".config-*")
	if err != nil {
		return fmt.Errorf("create config update: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write config update: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close config update: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace config: %w", err)
	}
	return nil
}

func normalizePRManager(prManager PRManager) (PRManager, error) {
	prManager.UserTokenEnv = strings.TrimSpace(prManager.UserTokenEnv)
	if prManager.UserTokenEnv == "" {
		prManager.UserTokenEnv = "BIFROST_SLACK_USER_TOKEN"
	}
	prManager.Conversation = strings.TrimSpace(prManager.Conversation)
	prManager.BotID = strings.TrimSpace(prManager.BotID)
	if !validEnvironmentName(prManager.UserTokenEnv) {
		return PRManager{}, fmt.Errorf("pr_manager.user_token_env must contain only letters, digits, and underscores")
	}
	if prManager.Enabled && (prManager.Conversation == "" || prManager.BotID == "") {
		return PRManager{}, fmt.Errorf("pr_manager.conversation_id and pr_manager.bot_id are required when pr_manager is enabled")
	}
	return prManager, nil
}

func validEnvironmentName(value string) bool {
	for index, character := range value {
		if character == '_' || character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' || index > 0 && character >= '0' && character <= '9' {
			continue
		}
		return false
	}
	return value != ""
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
