package harness

import (
	"context"
	"errors"
)

var ErrSessionNotFound = errors.New("harness session not found")

var ErrAmbiguousSession = errors.New("multiple harness sessions match the pull request")

var ErrDiscoveryDeferred = errors.New("harness discovery deferred after a batch failure")

type Target struct {
	Repository       string
	PullRequest      int
	URL              string
	HeadRef          string
	WorkingDirectory string
	// ExcludedSessionIDs must be removed from candidates before ambiguity is evaluated.
	ExcludedSessionIDs []string
}

type Session struct {
	ID string
}

type Discovery struct {
	Session Session
	Found   bool
	Err     error
}

type Request struct {
	SessionID        string
	WorkingDirectory string
	Prompt           string
}

type Result struct {
	SessionID string
}

// Harness implementations must support concurrent Discover and Dispatch calls.
// Discover owns and cleans up all provider resources used for a batch. It may
// reconnect within its deadline and retry budget, and returns one positionally
// corresponding result per target. A batch error means discovery could not
// start; target-specific and partial-batch failures belong in Discovery.Err.
type Harness interface {
	Name() string
	Discover(context.Context, []Target) ([]Discovery, error)
	Dispatch(context.Context, Request) (Result, error)
}
