package github

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestOpenPullRequestsFiltersAuthors(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/repos/owner/repo/pulls" {
			http.Error(writer, "wrong path", http.StatusBadRequest)
			return
		}
		requests.Add(1)
		if request.URL.Query().Get("per_page") != "100" {
			http.Error(writer, "wrong page size", http.StatusBadRequest)
			return
		}
		page := request.URL.Query().Get("page")
		if page == "2" {
			fmt.Fprint(writer, `[{"number":101,"title":"mine too","html_url":"https://example/pr/101","user":{"login":"matan"}}]`)
			return
		}
		if page != "1" {
			http.Error(writer, "wrong page", http.StatusBadRequest)
			return
		}
		rows := make([]map[string]any, githubPageSize)
		for index := range rows {
			login := "other"
			if index == 0 {
				login = "matan"
			}
			rows[index] = map[string]any{
				"number": index + 1, "title": "pr", "html_url": fmt.Sprintf("https://example/pr/%d", index+1),
				"user": map[string]string{"login": login},
			}
		}
		_ = json.NewEncoder(writer).Encode(rows)
	}))
	defer server.Close()
	client := NewClient("")
	client.restBaseURL = server.URL

	pullRequests, err := client.OpenPullRequests(context.Background(), "owner/repo", []string{"MATAN"})
	if err != nil {
		t.Fatal(err)
	}
	if len(pullRequests) != 2 || pullRequests[0].Number != 1 || pullRequests[1].Number != 101 || requests.Load() != 2 {
		t.Fatalf("pull requests = %#v", pullRequests)
	}
}

func TestReviewThreadsPaginatesThreadsAndComments(t *testing.T) {
	t.Parallel()
	var requestMutex sync.Mutex
	var threadCursors []string
	var commentCursors []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Query     string         `json:"query"`
			Variables map[string]any `json:"variables"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, "bad request", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		cursor := ""
		if body.Variables["after"] != nil {
			cursor, _ = body.Variables["after"].(string)
		}
		switch {
		case strings.Contains(body.Query, "PullRequestReviewThreadComments"):
			requestMutex.Lock()
			commentCursors = append(commentCursors, cursor)
			requestMutex.Unlock()
			if cursor == "comment-page-2" {
				fmt.Fprint(writer, `{"data":{"node":{"comments":{"pageInfo":{"hasNextPage":true,"endCursor":"comment-page-3"},"nodes":[{"id":"comment-2","author":{"login":"bob"},"bodyText":"reply","url":"https://example/comment/2","createdAt":"2026-08-20T10:01:00Z","updatedAt":"2026-08-20T10:01:00Z"}]}}}}`)
				return
			}
			if cursor != "comment-page-3" {
				http.Error(writer, "wrong comment cursor", http.StatusBadRequest)
				return
			}
			fmt.Fprint(writer, `{"data":{"node":{"comments":{"pageInfo":{"hasNextPage":false,"endCursor":""},"nodes":[{"id":"comment-3","author":{"login":"carol"},"bodyText":"last reply","url":"https://example/comment/3","createdAt":"2026-08-20T10:02:00Z","updatedAt":"2026-08-20T10:02:00Z"}]}}}}`)
		case body.Variables["after"] == nil:
			requestMutex.Lock()
			threadCursors = append(threadCursors, cursor)
			requestMutex.Unlock()
			fmt.Fprint(writer, `{"data":{"repository":{"pullRequest":{"reviewThreads":{"pageInfo":{"hasNextPage":true,"endCursor":"thread-page-2"},"nodes":[{"id":"thread-b","isResolved":false,"isOutdated":true,"path":"main.go","line":null,"startLine":null,"originalLine":12,"originalStartLine":10,"diffSide":"RIGHT","startDiffSide":"RIGHT","comments":{"pageInfo":{"hasNextPage":true,"endCursor":"comment-page-2"},"nodes":[{"id":"comment-1","author":{"login":"alice"},"bodyText":"fix this","url":"https://example/comment/1","createdAt":"2026-08-20T10:00:00Z","updatedAt":"2026-08-20T10:00:00Z"}]}}]}}}}}`)
		case cursor == "thread-page-2":
			requestMutex.Lock()
			threadCursors = append(threadCursors, cursor)
			requestMutex.Unlock()
			fmt.Fprint(writer, `{"data":{"repository":{"pullRequest":{"reviewThreads":{"pageInfo":{"hasNextPage":true,"endCursor":"thread-page-3"},"nodes":[{"id":"thread-c","isResolved":false,"isOutdated":false,"path":"middle.go","line":8,"startLine":null,"originalLine":8,"originalStartLine":null,"diffSide":"RIGHT","startDiffSide":"","comments":{"pageInfo":{"hasNextPage":false,"endCursor":""},"nodes":[]}}]}}}}}`)
		case cursor == "thread-page-3":
			requestMutex.Lock()
			threadCursors = append(threadCursors, cursor)
			requestMutex.Unlock()
			fmt.Fprint(writer, `{"data":{"repository":{"pullRequest":{"reviewThreads":{"pageInfo":{"hasNextPage":false,"endCursor":""},"nodes":[{"id":"thread-a","isResolved":true,"isOutdated":false,"path":"other.go","line":5,"startLine":null,"originalLine":5,"originalStartLine":null,"diffSide":"RIGHT","startDiffSide":"","comments":{"pageInfo":{"hasNextPage":false,"endCursor":""},"nodes":[]}}]}}}}}`)
		default:
			http.Error(writer, "wrong thread cursor", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	client := NewClient("")
	client.graphQLURL = server.URL

	threads, err := client.ReviewThreads(context.Background(), PullRequest{Repository: "owner/repo", Number: 42})
	if err != nil {
		t.Fatal(err)
	}
	if len(threads) != 3 || threads[0].ID != "thread-a" || threads[1].ID != "thread-b" || threads[2].ID != "thread-c" {
		t.Fatalf("threads = %#v", threads)
	}
	thread := threads[1]
	if !thread.IsOutdated || thread.OriginalLine == nil || *thread.OriginalLine != 12 {
		t.Fatalf("thread location = %#v", thread)
	}
	if len(thread.Comments) != 3 || thread.Comments[2].Body != "last reply" {
		t.Fatalf("comments = %#v", thread.Comments)
	}
	requestMutex.Lock()
	defer requestMutex.Unlock()
	wantThreadCursors := []string{"", "thread-page-2", "thread-page-3"}
	wantCommentCursors := []string{"comment-page-2", "comment-page-3"}
	if fmt.Sprint(threadCursors) != fmt.Sprint(wantThreadCursors) || fmt.Sprint(commentCursors) != fmt.Sprint(wantCommentCursors) {
		t.Fatalf("thread/comment cursors = %#v / %#v", threadCursors, commentCursors)
	}
}
