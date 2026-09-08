package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"strings"
	"time"
)

const (
	codexDiscoveryBaseTimeout = 30 * time.Second
	codexDiscoveryPerTarget   = 3 * time.Second
	maxCodexDiscoveryTimeout  = 5 * time.Minute
	threadListPageSize        = 100
	maxThreadListPages        = 100
	maxIgnoredRPCMessages     = 100
	maxAppServerOutputBytes   = 64 * 1024 * 1024
	maxAppServerReconnects    = 3
	bifrostTaskNamePrefix     = "Bifrost: "
)

var errAppServerOutputLimit = errors.New("Codex app-server output limit exceeded")

var codexTaskSources = []string{
	"cli",
	"vscode",
	"exec",
	"appServer",
	"subAgent",
	"subAgentReview",
	"subAgentCompact",
	"subAgentThreadSpawn",
	"subAgentOther",
	"unknown",
}

type codexAppServer struct {
	command     string
	environment []string
	processes   processStarter
}

type appServerClient struct {
	encoder        *json.Encoder
	decoder        *json.Decoder
	nextID         int
	sessionsByName map[string]map[string]bool
	sessionsLoaded bool
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code int `json:"code"`
	} `json:"error"`
}

type threadListThread struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type threadListResult struct {
	Data       *[]threadListThread `json:"data"`
	NextCursor *string             `json:"nextCursor"`
}

type threadForkResult struct {
	Thread struct {
		ID string `json:"id"`
	} `json:"thread"`
}

type fatalAppServerError struct{ err error }

func (err fatalAppServerError) Error() string { return err.err.Error() }
func (err fatalAppServerError) Unwrap() error { return err.err }

type rpcRequestError struct {
	code int
}

func (err rpcRequestError) Error() string {
	return fmt.Sprintf("Codex app-server request failed with code %d", err.code)
}

type boundedReader struct {
	reader    io.Reader
	remaining int64
}

func (reader *boundedReader) Read(value []byte) (int, error) {
	if reader.remaining <= 0 {
		return 0, errAppServerOutputLimit
	}
	if int64(len(value)) > reader.remaining {
		value = value[:reader.remaining]
	}
	count, err := reader.reader.Read(value)
	reader.remaining -= int64(count)
	return count, err
}

func (server *codexAppServer) Discover(ctx context.Context, targets []Target) ([]Discovery, error) {
	discoveries := make([]Discovery, len(targets))
	validTargets := 0
	for index, target := range targets {
		if strings.TrimSpace(target.Repository) == "" || target.PullRequest <= 0 {
			discoveries[index].Err = fmt.Errorf("repository and pull request number are required for Codex task discovery")
			continue
		}
		validTargets++
	}
	if validTargets == 0 {
		return discoveries, nil
	}

	discoveryTimeout := codexDiscoveryBaseTimeout + time.Duration(validTargets-1)*codexDiscoveryPerTarget
	if discoveryTimeout > maxCodexDiscoveryTimeout {
		discoveryTimeout = maxCodexDiscoveryTimeout
	}
	discoveryContext, cancel := context.WithTimeout(ctx, discoveryTimeout)
	defer cancel()
	return server.discoverBatch(discoveryContext, targets, discoveries, maxAppServerReconnects)
}

func (server *codexAppServer) discoverBatch(ctx context.Context, targets []Target, discoveries []Discovery, reconnects int) ([]Discovery, error) {
	processContext, cancel := context.WithCancel(ctx)
	defer cancel()
	process, err := server.processes.Start(processContext, processRequest{
		Command: server.command, Args: []string{"app-server", "--stdio"},
		Environment: server.environment, Interactive: true,
	})
	if err != nil {
		return nil, fmt.Errorf("start Codex app-server: %w", err)
	}

	reader := &boundedReader{reader: process.Output(), remaining: maxAppServerOutputBytes}
	client := newAppServerClient(reader, process.Input())
	protocolError := client.initialize()
	fatalTargetIndex := -1
	if protocolError == nil {
		for index, target := range targets {
			if discoveries[index].Err != nil {
				continue
			}
			discoveries[index] = client.discover(target)
			var fatalError fatalAppServerError
			if errors.As(discoveries[index].Err, &fatalError) {
				protocolError = discoveries[index].Err
				fatalTargetIndex = index
				break
			}
		}
	}
	contextErrorBeforeCleanup := processContext.Err()
	if protocolError != nil {
		cancel()
	}
	_ = process.Input().Close()
	exit := process.Wait()

	var discoveryError error
	if contextErrorBeforeCleanup != nil {
		discoveryError = discoveryProcessError(contextErrorBeforeCleanup, exit.CleanupFailed, exit.PipeCleanupTimedOut, exit.WaitError)
	} else if protocolError != nil {
		if exit.CleanupFailed || exit.PipeCleanupTimedOut || errors.Is(exit.WaitError, exec.ErrWaitDelay) {
			discoveryError = errors.Join(protocolError, fmt.Errorf("Codex app-server cleanup failed"))
		} else {
			discoveryError = protocolError
		}
	} else if exit.WaitError != nil {
		if exit.ContextError != nil {
			discoveryError = discoveryProcessError(exit.ContextError, exit.CleanupFailed, exit.PipeCleanupTimedOut, exit.WaitError)
		} else {
			discoveryError = fmt.Errorf("Codex app-server exited unsuccessfully")
		}
	}
	if discoveryError == nil {
		return discoveries, nil
	}
	if fatalTargetIndex < 0 {
		return nil, discoveryError
	}
	discoveries[fatalTargetIndex].Err = discoveryError
	cleanReap := contextErrorBeforeCleanup == nil && !exit.CleanupFailed && !exit.PipeCleanupTimedOut && !errors.Is(exit.WaitError, exec.ErrWaitDelay)
	remainingNeedsDiscovery := slices.ContainsFunc(discoveries[fatalTargetIndex+1:], func(discovery Discovery) bool {
		return discovery.Err == nil
	})
	if reconnects > 0 && cleanReap && ctx.Err() == nil && remainingNeedsDiscovery {
		remaining, remainingError := server.discoverBatch(ctx, targets[fatalTargetIndex+1:], discoveries[fatalTargetIndex+1:], reconnects-1)
		if remainingError == nil {
			copy(discoveries[fatalTargetIndex+1:], remaining)
			return discoveries, nil
		}
		discoveryError = remainingError
	}
	for index := fatalTargetIndex + 1; index < len(discoveries); index++ {
		if discoveries[index].Err == nil {
			discoveries[index].Err = fmt.Errorf("%w: %v", ErrDiscoveryDeferred, discoveryError)
		}
	}
	return discoveries, nil
}

func discoveryProcessError(contextError error, cleanupFailed, pipeCleanupTimedOut bool, waitError error) error {
	if cleanupFailed {
		return fmt.Errorf("discover Codex task: %w: app-server cleanup failed", contextError)
	}
	if pipeCleanupTimedOut || errors.Is(waitError, exec.ErrWaitDelay) {
		return fmt.Errorf("discover Codex task: %w: app-server cleanup timed out", contextError)
	}
	return fmt.Errorf("discover Codex task: %w", contextError)
}

func newAppServerClient(reader io.Reader, writer io.Writer) *appServerClient {
	return &appServerClient{encoder: json.NewEncoder(writer), decoder: json.NewDecoder(reader)}
}

func (client *appServerClient) discover(target Target) Discovery {
	creatorTaskName := strings.TrimSpace(target.TaskName)
	if creatorTaskName == "" {
		return Discovery{}
	}
	creatorSessions, err := client.exactNamedSessions(creatorTaskName, target.ExcludedSessionIDs)
	if err != nil {
		return Discovery{Err: err}
	}
	if len(creatorSessions) > 1 {
		return Discovery{Err: fmt.Errorf("%w: found at least 2 tasks named %q", ErrAmbiguousSession, creatorTaskName)}
	}
	if len(creatorSessions) == 0 {
		return Discovery{}
	}
	forkedSessionID, err := client.fork(creatorSessions[0], bifrostTaskName(target))
	if err != nil {
		return Discovery{Err: err}
	}
	return Discovery{Session: Session{ID: forkedSessionID}, Found: true}
}

func bifrostTaskName(target Target) string {
	return fmt.Sprintf("%s%s#%d", bifrostTaskNamePrefix, strings.ToLower(target.Repository), target.PullRequest)
}

func (client *appServerClient) fork(sessionID, taskName string) (string, error) {
	var result threadForkResult
	if err := client.request("thread/fork", map[string]any{"threadId": sessionID}, &result); err != nil {
		return "", err
	}
	if !validSessionID(result.Thread.ID) {
		return "", fmt.Errorf("Codex app-server returned an invalid forked thread ID")
	}
	if strings.EqualFold(result.Thread.ID, sessionID) {
		return "", fmt.Errorf("Codex app-server returned the creator thread as the fork")
	}
	var nameResult struct{}
	if err := client.request("thread/name/set", map[string]any{
		"threadId": result.Thread.ID,
		"name":     taskName,
	}, &nameResult); err != nil {
		var deleteResult struct{}
		deleteError := client.request("thread/delete", map[string]any{"threadId": result.Thread.ID}, &deleteResult)
		return "", errors.Join(err, deleteError)
	}
	return result.Thread.ID, nil
}

func (client *appServerClient) initialize() error {
	var result struct{}
	if err := client.request("initialize", map[string]any{
		"clientInfo":   map[string]string{"name": "bifrost", "version": "1"},
		"capabilities": map[string]bool{"experimentalApi": true},
	}, &result); err != nil {
		return err
	}
	if err := client.encoder.Encode(map[string]any{
		"method": "initialized", "params": map[string]any{},
	}); err != nil {
		return fatalAppServerError{err: fmt.Errorf("write Codex app-server notification: %w", err)}
	}
	return nil
}

func (client *appServerClient) exactNamedSessions(name string, excludedSessionIDs []string) ([]string, error) {
	if err := client.loadNamedSessions(); err != nil {
		return nil, err
	}
	matching := make([]string, 0, len(client.sessionsByName[name]))
	for sessionID := range client.sessionsByName[name] {
		if slices.ContainsFunc(excludedSessionIDs, func(excluded string) bool {
			return strings.EqualFold(sessionID, strings.TrimSpace(excluded))
		}) {
			continue
		}
		matching = append(matching, sessionID)
	}
	return matching, nil
}

func (client *appServerClient) loadNamedSessions() error {
	if client.sessionsLoaded {
		return nil
	}
	sessions := make(map[string]map[string]bool)
	for _, archived := range []bool{false, true} {
		if err := client.listSessions(archived, sessions); err != nil {
			return err
		}
	}
	client.sessionsByName = sessions
	client.sessionsLoaded = true
	return nil
}

func (client *appServerClient) listSessions(archived bool, sessions map[string]map[string]bool) error {
	var cursor *string
	seenCursors := make(map[string]bool)
	for page := 0; page < maxThreadListPages; page++ {
		params := map[string]any{
			"limit":       threadListPageSize,
			"archived":    archived,
			"sourceKinds": codexTaskSources,
		}
		if cursor != nil {
			params["cursor"] = *cursor
		}
		var result threadListResult
		if err := client.request("thread/list", params, &result); err != nil {
			return err
		}
		if result.Data == nil {
			return fmt.Errorf("Codex app-server returned malformed thread list data")
		}
		for _, thread := range *result.Data {
			if !validSessionID(thread.ID) {
				return fmt.Errorf("Codex app-server returned an invalid thread ID")
			}
			if thread.Name == "" {
				continue
			}
			if sessions[thread.Name] == nil {
				sessions[thread.Name] = make(map[string]bool)
			}
			sessions[thread.Name][thread.ID] = true
		}
		if result.NextCursor == nil || *result.NextCursor == "" {
			return nil
		}
		if seenCursors[*result.NextCursor] {
			return fmt.Errorf("Codex app-server repeated a thread list cursor")
		}
		seenCursors[*result.NextCursor] = true
		cursor = result.NextCursor
	}
	return fmt.Errorf("Codex task discovery exceeded %d list pages", maxThreadListPages)
}

func (client *appServerClient) request(method string, params any, result any) error {
	client.nextID++
	requestID := client.nextID
	if err := client.encoder.Encode(map[string]any{
		"id": requestID, "method": method, "params": params,
	}); err != nil {
		return fatalAppServerError{err: fmt.Errorf("write Codex app-server request: %w", err)}
	}
	for ignored := 0; ignored <= maxIgnoredRPCMessages; ignored++ {
		var response rpcResponse
		if err := client.decoder.Decode(&response); err != nil {
			if errors.Is(err, io.EOF) {
				return fatalAppServerError{err: fmt.Errorf("Codex app-server closed before responding")}
			}
			return fatalAppServerError{err: fmt.Errorf("read Codex app-server response: %w", err)}
		}
		if response.ID != requestID {
			continue
		}
		if response.JSONRPC != "" && response.JSONRPC != "2.0" {
			return fmt.Errorf("Codex app-server returned an invalid JSON-RPC version")
		}
		if response.Error != nil {
			return rpcRequestError{code: response.Error.Code}
		}
		if len(response.Result) == 0 || string(response.Result) == "null" {
			return fmt.Errorf("Codex app-server returned an empty result")
		}
		if err := json.Unmarshal(response.Result, result); err != nil {
			return fmt.Errorf("decode Codex app-server response: %w", err)
		}
		return nil
	}
	return fatalAppServerError{err: fmt.Errorf("Codex app-server sent too many unrelated messages")}
}
