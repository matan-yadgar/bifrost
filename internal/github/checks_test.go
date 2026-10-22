package github

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestFailedChecksUsesCurrentHeadLatestAttemptsAndContexts(t *testing.T) {
	t.Parallel()
	var attempt atomic.Int64
	attempt.Store(1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/repos/owner/repo/commits/current-sha/check-runs":
			if request.URL.Query().Get("filter") != "latest" {
				t.Error("missing latest-attempt filter")
			}
			_, _ = fmt.Fprintf(writer, `{"check_runs":[{"id":1,"completed_at":"2026-10-22T12:00:%02dZ","name":"unit","status":"completed","conclusion":"failure","html_url":"https://example/unit"},{"status":"in_progress","conclusion":"failure"},{"status":"completed","conclusion":"cancelled"},{"status":"completed","conclusion":"success"},{"id":5,"name":"build","status":"completed","conclusion":"timed_out"}]}`, attempt.Load())
		case "/repos/owner/repo/commits/current-sha/statuses":
			_, _ = fmt.Fprintf(writer, `[{"context":"external","state":"pending"},{"context":"external","state":"failure"},{"id":%d,"context":"deploy","state":"error","target_url":"https://example/deploy"}]`, 7+attempt.Load())
		default:
			t.Errorf("unexpected endpoint %s", request.URL)
			http.Error(writer, "unexpected endpoint", 400)
		}
	}))
	defer server.Close()
	client := NewClient("")
	client.restBaseURL = server.URL
	checks, err := client.FailedChecks(context.Background(), PullRequest{Repository: "owner/repo", HeadSHA: "current-sha"})
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) != 3 || checks[0].Name != "unit" || checks[1].Name != "build" || checks[2].Name != "deploy" {
		t.Fatalf("checks=%#v", checks)
	}
	repeated, err := client.FailedChecks(context.Background(), PullRequest{Repository: "owner/repo", HeadSHA: "current-sha"})
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].ID != repeated[0].ID || checks[2].ID != repeated[2].ID {
		t.Fatal("unchanged attempts have unstable identities")
	}
	attempt.Store(2)
	rerun, err := client.FailedChecks(context.Background(), PullRequest{Repository: "owner/repo", HeadSHA: "current-sha"})
	if err != nil {
		t.Fatal(err)
	}
	if checks[0].ID == rerun[0].ID || checks[2].ID == rerun[2].ID {
		t.Fatal("new attempts reuse previously reported identities")
	}
}

func TestFailedChecksDoesNotReturnPartialScan(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("page") == "2" {
			http.Error(writer, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if request.URL.Path == "/repos/owner/repo/commits/sha/check-runs" {
			_, _ = fmt.Fprint(writer, `{"check_runs":[`)
			for index := range 100 {
				if index > 0 {
					_, _ = fmt.Fprint(writer, ",")
				}
				_, _ = fmt.Fprint(writer, `{"status":"completed","conclusion":"failure"}`)
			}
			_, _ = fmt.Fprint(writer, ` ]}`)
			return
		}
		t.Error("statuses must not be queried after partial check failure")
	}))
	defer server.Close()
	client := NewClient("")
	client.restBaseURL = server.URL
	checks, err := client.FailedChecks(context.Background(), PullRequest{Repository: "owner/repo", HeadSHA: "sha"})
	if err == nil || checks != nil {
		t.Fatalf("accepted partial result: %#v %v", checks, err)
	}
}
