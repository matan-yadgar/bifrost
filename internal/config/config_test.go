package config

import (
	"encoding/json"
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
	if runtimeConfig.MappingDirectory != filepath.Join(directory, "mappings") {
		t.Fatalf("mapping directory = %q", runtimeConfig.MappingDirectory)
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
}

func TestLoadRejectsLegacyMappingFile(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		configJSON string
		legacyFile bool
	}{
		{
			name:       "configured legacy path",
			configJSON: `{"mapping_file":"old.json","repositories":[{"name":"owner/repo","working_directory":"repo"}]}`,
		},
		{
			name:       "default legacy path exists",
			configJSON: `{"repositories":[{"name":"owner/repo","working_directory":"repo"}]}`,
			legacyFile: true,
		},
	} {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			directory := t.TempDir()
			if err := os.Mkdir(filepath.Join(directory, "repo"), 0o700); err != nil {
				t.Fatal(err)
			}
			if testCase.legacyFile {
				if err := os.WriteFile(filepath.Join(directory, "mappings.json"), []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			configPath := filepath.Join(directory, "config.json")
			if err := os.WriteFile(configPath, []byte(testCase.configJSON), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(configPath); err == nil {
				t.Fatal("expected legacy mapping error")
			}
		})
	}
}

func TestLoadRejectsPathCollisions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		stateFile        string
		mappingDirectory string
	}{
		{name: "state and mapping", stateFile: "shared", mappingDirectory: "shared"},
		{name: "state and config", stateFile: "config.json", mappingDirectory: "mappings"},
		{name: "mapping and config", stateFile: "state.json", mappingDirectory: "config.json"},
		{name: "state beneath mapping", stateFile: "mappings/owner/repo/42.json", mappingDirectory: "mappings"},
		{name: "mapping beneath state", stateFile: "data", mappingDirectory: "data/mappings"},
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
  "mapping_directory": "` + test.mappingDirectory + `",
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
