package config

import (
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

func TestLoadRejectsPathCollisions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		stateFile   string
		mappingFile string
	}{
		{name: "state and mapping", stateFile: "shared.json", mappingFile: "shared.json"},
		{name: "state and config", stateFile: "config.json", mappingFile: "mappings.json"},
		{name: "mapping and config", stateFile: "state.json", mappingFile: "config.json"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			workingDirectory := filepath.Join(directory, "repo")
			if err := os.Mkdir(workingDirectory, 0o700); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(directory, "config.json")
			configJSON := `{
  "state_file": "` + test.stateFile + `",
  "mapping_file": "` + test.mappingFile + `",
  "repositories": [{"name":"owner/repo","working_directory":"repo"}],
  "harness": {"type":"codex"}
}`
			if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(configPath); err == nil {
				t.Fatal("expected path collision error")
			}
		})
	}
}
