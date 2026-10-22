package github

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFailedChecksUsesCurrentHeadLatestAttemptsAndContexts(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/repos/owner/repo/commits/current-sha/check-runs":
			if request.URL.Query().Get("filter") != "latest" {
				t.Error("missing latest-attempt filter")
			}
			_, _ = fmt.Fprint(writer, `{"check_runs":[{"name":"unit","status":"completed","conclusion":"failure","html_url":"https://example/unit"},{"status":"in_progress","conclusion":"failure"},{"status":"completed","conclusion":"cancelled"},{"status":"completed","conclusion":"success"},{"name":"build","status":"completed","conclusion":"timed_out"}]}`)
		case "/repos/owner/repo/commits/current-sha/statuses":
			_, _ = fmt.Fprint(writer, `[{"context":"external","state":"pending"},{"context":"external","state":"failure"},{"context":"deploy","state":"error","target_url":"https://example/deploy"}]`)
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
