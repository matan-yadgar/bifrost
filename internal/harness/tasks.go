package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ReadTask reads persisted history without taking ownership of a Desktop thread.
func (codex *Codex) ReadTask(ctx context.Context, identifier string) (json.RawMessage, error) {
	raw, err := codex.appServer.ReadTask(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return recentTaskHistory(raw)
}

func (server *codexAppServer) ReadTask(ctx context.Context, identifier string) (json.RawMessage, error) {
	process, err := server.processes.Start(ctx, processRequest{Command: server.command, Args: []string{"app-server", "--stdio"}, Environment: server.environment, Interactive: true})
	if err != nil {
		return nil, err
	}
	client := newAppServerClient(&boundedReader{reader: process.Output(), remaining: maxAppServerOutputBytes}, process.Input())
	var task json.RawMessage
	err = client.initialize()
	if err == nil && !validSessionID(identifier) {
		var sessions []string
		sessions, err = client.exactNamedSessions(identifier, nil)
		if err == nil {
			if len(sessions) != 1 {
				err = fmt.Errorf("expected one task named %q; found %d", identifier, len(sessions))
			} else {
				identifier = sessions[0]
			}
		}
	}
	if err == nil {
		err = client.request("thread/read", map[string]any{"threadId": identifier, "includeTurns": true}, &task)
	}
	_ = process.Input().Close()
	exit := process.Wait()
	if err != nil {
		return nil, err
	}
	if exit.WaitError != nil {
		return nil, fmt.Errorf("read Codex task: app-server exited unsuccessfully")
	}
	var metadata struct {
		Thread struct {
			ID        string `json:"id"`
			UpdatedAt int64  `json:"updatedAt"`
			Turns     []struct {
				ID     string `json:"id"`
				Status string `json:"status"`
			} `json:"turns"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(task, &metadata); err != nil {
		return nil, err
	}
	if metadata.Thread.ID != identifier {
		return nil, fmt.Errorf("Codex returned unexpected task ID")
	}
	revision := fmt.Sprintf("%d", metadata.Thread.UpdatedAt)
	if len(metadata.Thread.Turns) > 0 {
		latest := metadata.Thread.Turns[len(metadata.Thread.Turns)-1]
		revision += ":" + latest.ID + ":" + latest.Status
	}
	var response map[string]json.RawMessage
	if err := json.Unmarshal(task, &response); err != nil {
		return nil, err
	}
	response["revision"], err = json.Marshal(revision)
	if err != nil {
		return nil, err
	}
	return json.Marshal(response)
}

// WakeTask starts an unloaded task, or queues to its existing writer.
func (codex *Codex) WakeTask(ctx context.Context, id, message string) (string, error) {
	if !validSessionID(id) || strings.TrimSpace(message) == "" {
		return "", fmt.Errorf("a task UUID and message are required")
	}
	_, err := codex.Dispatch(ctx, Request{SessionID: id, Prompt: message})
	if err == nil {
		return "resumed", nil
	}
	var failure *commandError
	if !errors.As(err, &failure) || !strings.Contains(strings.ToLower(failure.stderr), "already has an active writer") {
		return "", err
	}
	arguments := []string{"queue", "--thread", id, "--message", message}
	process, err := codex.appServer.processes.Start(ctx, processRequest{Command: codex.command, Args: arguments, Environment: codex.appServer.environment})
	if err != nil {
		return "", err
	}
	output, readErr := io.ReadAll(io.LimitReader(process.Output(), maxLaunchOutputBytes+1))
	exit := process.Wait()
	if err := commandRunError(exit, readErr); err != nil {
		return "", err
	}
	if len(output) > int(maxLaunchOutputBytes) || !strings.Contains(string(output), "Queued message ") {
		return "", fmt.Errorf("Codex queue returned no delivery receipt")
	}
	return "queued", nil
}

func (codex *Codex) Archive(ctx context.Context, id string) error {
	if !validSessionID(id) {
		return fmt.Errorf("invalid Codex task ID")
	}
	if err := codex.runner.Run(ctx, codex.command, []string{"archive", id}, "", func(string) {}); err != nil {
		return fmt.Errorf("archive task %s: %w", id, err)
	}
	return nil
}

func recentTaskHistory(raw json.RawMessage) (json.RawMessage, error) {
	var result struct {
		Revision string `json:"revision"`
		Thread   struct {
			ID     string          `json:"id"`
			Status json.RawMessage `json:"status"`
			Turns  []struct {
				ID     string            `json:"id"`
				Status string            `json:"status"`
				Items  []json.RawMessage `json:"items"`
			} `json:"turns"`
		} `json:"thread"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	turns := result.Thread.Turns
	if len(turns) > 8 {
		turns = turns[len(turns)-8:]
	}
	for index := range turns {
		var messages []json.RawMessage
		for _, item := range turns[index].Items {
			var header struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(item, &header); err != nil {
				return nil, err
			}
			if header.Type == "userMessage" || header.Type == "agentMessage" {
				messages = append(messages, item)
			}
		}
		turns[index].Items = messages
	}
	result.Thread.Turns = turns
	return json.Marshal(result)
}
