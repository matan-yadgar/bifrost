package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "repo")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	configJSON := `{
  "repositories": [{
    "name": "Owner/Repo",
    "authors": ["@Matan", "matan", ""],
    "working_directory": "repo"
  }],
  "harness": {"type": "codex"}
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	runtimeConfig, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.PollInterval != 5*time.Minute {
		t.Fatalf("poll interval = %s", runtimeConfig.PollInterval)
	}
	if runtimeConfig.DispatchTimeout != 30*time.Minute {
		t.Fatalf("dispatch timeout = %s", runtimeConfig.DispatchTimeout)
	}
	canonicalWorkingDirectory, err := filepath.EvalSymlinks(workingDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.Repositories[0].WorkingDirectory != canonicalWorkingDirectory {
		t.Fatalf("working directory = %q", runtimeConfig.Repositories[0].WorkingDirectory)
	}
	if len(runtimeConfig.Repositories[0].Authors) != 1 || runtimeConfig.Repositories[0].Authors[0] != "Matan" {
		t.Fatalf("authors = %#v", runtimeConfig.Repositories[0].Authors)
	}
	if runtimeConfig.Harness.Command != "codex" {
		t.Fatalf("command = %q", runtimeConfig.Harness.Command)
	}
	if runtimeConfig.StateFile != filepath.Join(directory, "state.json") {
		t.Fatalf("state file = %q", runtimeConfig.StateFile)
	}
	if runtimeConfig.LogDirectory != filepath.Join(directory, "logs") {
		t.Fatalf("log directory = %q", runtimeConfig.LogDirectory)
	}
}

func TestLoadRejectsDuplicateRepositories(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "repo")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	configJSON := `{
  "repositories": [
    {"name":"Owner/Repo","working_directory":"repo"},
    {"name":"owner/repo","working_directory":"repo"}
  ]
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(configPath); err == nil {
		t.Fatal("expected duplicate repository error")
	}
}

func TestLoadRejectsRepositoryNamesThatCannotBeMapped(t *testing.T) {
	t.Parallel()
	for _, repositoryName := range []string{"../repo", "owner/.", `owner/repo\name`, "owner/repo#1"} {
		repositoryName := repositoryName
		t.Run(repositoryName, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			if err := os.Mkdir(filepath.Join(directory, "repo"), 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(directory, "config.json")
			configJSON := `{"repositories":[{"name":` + quotedJSON(repositoryName) + `,"working_directory":"repo"}]}`
			if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(configPath); err == nil {
				t.Fatal("expected invalid repository error")
			}
		})
	}
}

func quotedJSON(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func TestLoadRejectsInvalidDispatchTimeout(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "repo")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	configJSON := `{
  "dispatch_timeout": "0s",
  "repositories": [{"name":"Owner/Repo","working_directory":"repo"}]
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(configPath); err == nil {
		t.Fatal("expected dispatch timeout error")
	}
}

func TestLoadUsesConfiguredDispatchTimeout(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "repo")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	configJSON := `{
  "dispatch_timeout": "47s",
  "log_directory": "custom-logs",
  "repositories": [{"name":"Owner/Repo","working_directory":"repo"}]
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.DispatchTimeout != 47*time.Second {
		t.Fatalf("dispatch timeout = %s", runtimeConfig.DispatchTimeout)
	}
	if runtimeConfig.LogDirectory != filepath.Join(directory, "custom-logs") {
		t.Fatalf("log directory = %q", runtimeConfig.LogDirectory)
	}
}

func TestLoadRejectsPathCollisions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name                  string
		stateFile             string
		logDir                string
		stateMustRemainAbsent bool
	}{
		{name: "state is config", stateFile: "config.json"},
		{name: "logs beneath state file", stateFile: "state.json", logDir: "state.json/logs", stateMustRemainAbsent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			workingDirectory := filepath.Join(directory, "repo")
			if err := os.Mkdir(workingDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(directory, "config.json")
			configJSON := `{
  "state_file": ` + quotedJSON(test.stateFile) + `,
  "log_directory": ` + quotedJSON(test.logDir) + `,
  "repositories": [{"name":"owner/repo","working_directory":"repo"}],
  "harness": {"type":"codex"}
}`
			if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(configPath); err == nil {
				t.Fatal("expected path collision error")
			}
			if test.stateMustRemainAbsent {
				if _, err := os.Stat(filepath.Join(directory, test.stateFile)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("state path was created: %v", err)
				}
			}
		})
	}
}
