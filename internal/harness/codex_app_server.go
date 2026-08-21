package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	codexDiscoveryBaseTimeout = 30 * time.Second
	codexDiscoveryPerTarget   = 3 * time.Second
	maxCodexDiscoveryTimeout  = 5 * time.Minute
	threadSearchPageSize      = 100
	threadTurnsPageSize       = 100
	threadItemsPageSize       = 100
	maxThreadSearchPages      = 100
	maxThreadTurnsPages       = 100
	maxThreadItemsPages       = 100
	maxDiscoveryCandidates    = 1000
	maxIgnoredRPCMessages     = 100
	maxAppServerOutputBytes   = 64 * 1024 * 1024
	maxAppServerReconnects    = 3
	finalAssistantMessage     = "final_answer"
	agentMessageItem          = "agentMessage"
)

var (
	errAppServerOutputLimit     = errors.New("Codex app-server output limit exceeded")
	errFullTurnItemsUnavailable = errors.New("Codex app-server did not hydrate completed turn items")
)

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
	encoder *json.Encoder
	decoder *json.Decoder
	nextID  int
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type threadSearchItem struct {
	Thread struct {
		ID string `json:"id"`
	} `json:"thread"`
}

type threadSearchResult struct {
	Data       *[]threadSearchItem `json:"data"`
	NextCursor *string             `json:"nextCursor"`
}

type threadTurn struct {
	ID        string            `json:"id"`
	Status    string            `json:"status"`
	Items     *[]threadTurnItem `json:"items"`
	ItemsView string            `json:"itemsView"`
}

type threadTurnItem struct {
	Type  string  `json:"type"`
	Text  string  `json:"text"`
	Phase *string `json:"phase"`
}

type threadTurnsResult struct {
	Data       *[]threadTurn `json:"data"`
	NextCursor *string       `json:"nextCursor"`
}

type threadItemEntry struct {
	TurnID string         `json:"turnId"`
	Item   threadTurnItem `json:"item"`
}

type threadItemsResult struct {
	Data       *[]threadItemEntry `json:"data"`
	NextCursor *string            `json:"nextCursor"`
}

type fatalAppServerError struct{ err error }

func (err fatalAppServerError) Error() string { return err.err.Error() }
func (err fatalAppServerError) Unwrap() error { return err.err }

type rpcRequestError struct {
	code    int
	message string
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
		if strings.TrimSpace(target.URL) == "" || strings.TrimSpace(target.HeadRef) == "" {
			discoveries[index].Err = fmt.Errorf("pull request URL and head branch are required for Codex task discovery")
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
	urlSessions, err := client.searchAll(target.URL)
	if err != nil {
		return Discovery{Err: err}
	}
	var candidateIDs []string
	for sessionID := range urlSessions {
		if slices.ContainsFunc(target.ExcludedSessionIDs, func(excluded string) bool {
			return strings.EqualFold(sessionID, strings.TrimSpace(excluded))
		}) {
			continue
		}
		candidateIDs = append(candidateIDs, sessionID)
	}
	sort.Strings(candidateIDs)
	var matching []string
	for _, sessionID := range candidateIDs {
		qualifies, err := client.hasCreatorFinal(sessionID, target.URL, target.HeadRef)
		if err != nil {
			return Discovery{Err: err}
		}
		if qualifies {
			matching = append(matching, sessionID)
			if len(matching) == 2 {
				return Discovery{Err: fmt.Errorf("%w: found at least 2 matches", ErrAmbiguousSession)}
			}
		}
	}
	if len(matching) == 0 {
		return Discovery{}
	}
	if !validSessionID(matching[0]) {
		return Discovery{Err: fmt.Errorf("Codex discovery returned an invalid session ID")}
	}
	return Discovery{Session: Session{ID: matching[0]}, Found: true}
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

func (client *appServerClient) searchAll(term string) (map[string]bool, error) {
	sessions := make(map[string]bool)
	for _, archived := range []bool{false, true} {
		if err := client.search(term, archived, sessions); err != nil {
			return nil, err
		}
	}
	return sessions, nil
}

func (client *appServerClient) search(term string, archived bool, sessions map[string]bool) error {
	var cursor *string
	seenCursors := make(map[string]bool)
	for page := 0; page < maxThreadSearchPages; page++ {
		params := map[string]any{
			"searchTerm":  term,
			"limit":       threadSearchPageSize,
			"archived":    archived,
			"sourceKinds": codexTaskSources,
		}
		if cursor != nil {
			params["cursor"] = *cursor
		}
		var result threadSearchResult
		if err := client.request("thread/search", params, &result); err != nil {
			return err
		}
		if result.Data == nil {
			return fmt.Errorf("Codex app-server returned malformed thread search data")
		}
		for _, item := range *result.Data {
			if !validSessionID(item.Thread.ID) {
				return fmt.Errorf("Codex app-server returned an invalid thread ID")
			}
			sessions[item.Thread.ID] = true
			if len(sessions) > maxDiscoveryCandidates {
				return fmt.Errorf("Codex task discovery exceeded %d candidates", maxDiscoveryCandidates)
			}
		}
		if result.NextCursor == nil || *result.NextCursor == "" {
			return nil
		}
		if seenCursors[*result.NextCursor] {
			return fmt.Errorf("Codex app-server repeated a thread search cursor")
		}
		seenCursors[*result.NextCursor] = true
		cursor = result.NextCursor
	}
	return fmt.Errorf("Codex task discovery exceeded %d search pages", maxThreadSearchPages)
}

func (client *appServerClient) hasCreatorFinal(sessionID, pullRequestURL, headRef string) (bool, error) {
	qualified, err := client.creatorFinalInFullTurns(sessionID, pullRequestURL, headRef)
	var requestError rpcRequestError
	fallback := errors.Is(err, errFullTurnItemsUnavailable) ||
		(errors.As(err, &requestError) && (requestError.code == -32602 ||
			(requestError.code == -32600 && !strings.Contains(strings.ToLower(requestError.message), "unknown variant"))))
	if err == nil || !fallback {
		return qualified, err
	}
	completedTurns, err := client.completedTurns(sessionID)
	if err != nil || len(completedTurns) == 0 {
		return false, err
	}
	return client.creatorFinalInItems(sessionID, completedTurns, pullRequestURL, headRef)
}

func (client *appServerClient) creatorFinalInFullTurns(sessionID, pullRequestURL, headRef string) (bool, error) {
	var cursor *string
	seenCursors := make(map[string]bool)
	for page := 0; page < maxThreadTurnsPages; page++ {
		params := map[string]any{
			"threadId": sessionID, "limit": threadTurnsPageSize,
			"sortDirection": "desc", "itemsView": "full",
		}
		if cursor != nil {
			params["cursor"] = *cursor
		}
		var result threadTurnsResult
		if err := client.request("thread/turns/list", params, &result); err != nil {
			return false, err
		}
		if result.Data == nil {
			return false, fmt.Errorf("Codex app-server returned malformed thread turns data")
		}
		for _, turn := range *result.Data {
			if turn.Status == "completed" && (turn.Items == nil || (turn.ItemsView != "" && turn.ItemsView != "full")) {
				return false, errFullTurnItemsUnavailable
			}
			if creatorFinalInTurn(turn, pullRequestURL, headRef) {
				return true, nil
			}
		}
		if result.NextCursor == nil || *result.NextCursor == "" {
			return false, nil
		}
		if seenCursors[*result.NextCursor] {
			return false, fmt.Errorf("Codex app-server repeated a thread turns cursor")
		}
		seenCursors[*result.NextCursor] = true
		cursor = result.NextCursor
	}
	return false, fmt.Errorf("Codex task discovery exceeded %d turn pages", maxThreadTurnsPages)
}

func creatorFinalInTurn(turn threadTurn, pullRequestURL, headRef string) bool {
	if turn.Status != "completed" || turn.Items == nil {
		return false
	}
	lastAgentMessage := -1
	for index, item := range *turn.Items {
		if item.Type == agentMessageItem {
			lastAgentMessage = index
		}
		if creatorFinalMessageMatches(item, false, pullRequestURL, headRef) {
			return true
		}
	}
	if lastAgentMessage < 0 {
		return false
	}
	return creatorFinalMessageMatches((*turn.Items)[lastAgentMessage], true, pullRequestURL, headRef)
}

func (client *appServerClient) completedTurns(sessionID string) (map[string]bool, error) {
	completedTurns := make(map[string]bool)
	var cursor *string
	seenCursors := make(map[string]bool)
	for page := 0; page < maxThreadTurnsPages; page++ {
		params := map[string]any{
			"threadId": sessionID, "limit": threadTurnsPageSize,
			"sortDirection": "desc", "itemsView": "notLoaded",
		}
		if cursor != nil {
			params["cursor"] = *cursor
		}
		var result threadTurnsResult
		if err := client.request("thread/turns/list", params, &result); err != nil {
			return nil, err
		}
		if result.Data == nil {
			return nil, fmt.Errorf("Codex app-server returned malformed thread turns data")
		}
		for _, turn := range *result.Data {
			if strings.TrimSpace(turn.ID) == "" {
				return nil, fmt.Errorf("Codex app-server returned a turn without an ID")
			}
			if turn.Status == "completed" {
				completedTurns[turn.ID] = true
			}
		}
		if result.NextCursor == nil || *result.NextCursor == "" {
			return completedTurns, nil
		}
		if seenCursors[*result.NextCursor] {
			return nil, fmt.Errorf("Codex app-server repeated a thread turns cursor")
		}
		seenCursors[*result.NextCursor] = true
		cursor = result.NextCursor
	}
	return nil, fmt.Errorf("Codex task discovery exceeded %d turn pages", maxThreadTurnsPages)
}

func (client *appServerClient) creatorFinalInItems(sessionID string, completedTurns map[string]bool, pullRequestURL, headRef string) (bool, error) {
	lastAgentMessages := make(map[string]threadTurnItem)
	var cursor *string
	seenCursors := make(map[string]bool)
	for page := 0; page < maxThreadItemsPages; page++ {
		params := map[string]any{
			"threadId": sessionID, "limit": threadItemsPageSize, "sortDirection": "asc",
		}
		if cursor != nil {
			params["cursor"] = *cursor
		}
		var result threadItemsResult
		if err := client.request("thread/items/list", params, &result); err != nil {
			return false, err
		}
		if result.Data == nil {
			return false, fmt.Errorf("Codex app-server returned malformed thread items data")
		}
		for _, entry := range *result.Data {
			if !completedTurns[entry.TurnID] || entry.Item.Type != agentMessageItem {
				continue
			}
			lastAgentMessages[entry.TurnID] = entry.Item
			if creatorFinalMessageMatches(entry.Item, false, pullRequestURL, headRef) {
				return true, nil
			}
		}
		if result.NextCursor == nil || *result.NextCursor == "" {
			for _, item := range lastAgentMessages {
				if creatorFinalMessageMatches(item, true, pullRequestURL, headRef) {
					return true, nil
				}
			}
			return false, nil
		}
		if seenCursors[*result.NextCursor] {
			return false, fmt.Errorf("Codex app-server repeated a thread items cursor")
		}
		seenCursors[*result.NextCursor] = true
		cursor = result.NextCursor
	}
	return false, fmt.Errorf("Codex task discovery exceeded %d item pages", maxThreadItemsPages)
}

func creatorFinalMessageMatches(item threadTurnItem, terminal bool, pullRequestURL, headRef string) bool {
	if item.Type != agentMessageItem ||
		!containsExactIdentifier(item.Text, pullRequestURL) ||
		!containsExactIdentifier(item.Text, headRef) {
		return false
	}
	if item.Phase == nil {
		return terminal
	}
	return *item.Phase == finalAssistantMessage
}

func containsExactIdentifier(text, identifier string) bool {
	for offset := 0; ; {
		index := strings.Index(text[offset:], identifier)
		if index < 0 {
			return false
		}
		start := offset + index
		end := start + len(identifier)
		if identifierBoundaryBefore(text, start) && identifierBoundaryAfter(text, end) {
			return true
		}
		offset = start + 1
	}
}

func identifierBoundaryBefore(text string, index int) bool {
	if index == 0 {
		return true
	}
	runeValue, _ := utf8.DecodeLastRuneInString(text[:index])
	return !identifierRune(runeValue)
}

func identifierBoundaryAfter(text string, index int) bool {
	if index == len(text) {
		return true
	}
	runeValue, _ := utf8.DecodeRuneInString(text[index:])
	return !identifierRune(runeValue)
}

func identifierRune(runeValue rune) bool {
	return unicode.IsLetter(runeValue) || unicode.IsDigit(runeValue) || strings.ContainsRune("-._/", runeValue)
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
			return rpcRequestError{code: response.Error.Code, message: response.Error.Message}
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
