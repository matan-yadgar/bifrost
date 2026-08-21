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
}

func TestLoadAcceptsLegacyMappingPaths(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "repo")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	configJSON := `{
  "state_file": "state.json",
  "mapping_directory": "legacy-mappings",
  "mapping_file": "legacy-mappings.json",
  "repositories": [{"name":"owner/repo","working_directory":"repo"}]
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeConfig, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.LegacyMappingDirectory != filepath.Join(directory, "legacy-mappings") || runtimeConfig.LegacyMappingFile != filepath.Join(directory, "legacy-mappings.json") {
		t.Fatalf("legacy paths = %q / %q", runtimeConfig.LegacyMappingDirectory, runtimeConfig.LegacyMappingFile)
	}
}

func TestLoadDoesNotMistakeStateForImplicitLegacyMappingFile(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "repo")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "mappings.json"), []byte(`{"version":1,"threads":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	configJSON := `{
  "state_file": "mappings.json",
  "repositories": [{"name":"owner/repo","working_directory":"repo"}]
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	runtimeConfig, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.LegacyMappingFile != "" {
		t.Fatalf("implicit legacy mapping file = %q", runtimeConfig.LegacyMappingFile)
	}
}

func TestLoadDetectsLegacyDefaultPaths(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "repo")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	mappingDirectory := filepath.Join(directory, "mappings")
	if err := os.Mkdir(mappingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	mappingFile := filepath.Join(directory, "mappings.json")
	if err := os.WriteFile(mappingFile, []byte(`{"version":1,"pull_requests":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	configJSON := `{"repositories":[{"name":"owner/repo","working_directory":"repo"}]}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}

	runtimeConfig, err := Load(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if runtimeConfig.LegacyMappingDirectory != mappingDirectory || runtimeConfig.LegacyMappingFile != mappingFile {
		t.Fatalf("legacy default paths = %q / %q", runtimeConfig.LegacyMappingDirectory, runtimeConfig.LegacyMappingFile)
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

func TestLoadRejectsPathCollisions(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "repo")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	configJSON := `{
  "state_file": "config.json",
  "repositories": [{"name":"owner/repo","working_directory":"repo"}],
  "harness": {"type":"codex"}
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(configPath); err == nil {
		t.Fatal("expected path collision error")
	}
}
