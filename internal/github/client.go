// Portions of this file are adapted from github.com/danielwolfman/prdash.
// See THIRD_PARTY_NOTICES.md.
package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	githubPageSize           = 100
	maxGitHubResponseBytes   = 8 * 1024 * 1024
	maxReviewThreadDataBytes = 32 * 1024 * 1024
)

var tokenEnvironmentNames = []string{"GH_TOKEN", "GITHUB_TOKEN"}

const reviewThreadsQuery = `
query PullRequestReviewThreads($owner: String!, $repo: String!, $number: Int!, $first: Int!, $after: String, $commentsFirst: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      reviewThreads(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          isResolved
          isOutdated
          path
          line
          startLine
          originalLine
          originalStartLine
          diffSide
          startDiffSide
          comments(first: $commentsFirst) {
            pageInfo { hasNextPage endCursor }
            nodes {
              id
              author { login }
              bodyText
              url
              createdAt
              updatedAt
            }
          }
        }
      }
    }
  }
}`

const reviewThreadCommentsQuery = `
query PullRequestReviewThreadComments($id: ID!, $first: Int!, $after: String) {
  node(id: $id) {
    ... on PullRequestReviewThread {
      comments(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          author { login }
          bodyText
          url
          createdAt
          updatedAt
        }
      }
    }
  }
}`

type Client struct {
	token       string
	httpClient  *http.Client
	restBaseURL string
	graphQLURL  string
}

type PullRequest struct {
	Repository string
	Number     int
	Title      string
	URL        string
}

type ReviewThread struct {
	ID                string          `json:"id"`
	IsResolved        bool            `json:"is_resolved"`
	IsOutdated        bool            `json:"is_outdated"`
	Path              string          `json:"path"`
	Line              *int            `json:"line,omitempty"`
	StartLine         *int            `json:"start_line,omitempty"`
	OriginalLine      *int            `json:"original_line,omitempty"`
	OriginalStartLine *int            `json:"original_start_line,omitempty"`
	DiffSide          string          `json:"diff_side,omitempty"`
	StartDiffSide     string          `json:"start_diff_side,omitempty"`
	Comments          []ReviewComment `json:"comments"`
}

type ReviewComment struct {
	ID        string    `json:"id"`
	Author    string    `json:"author"`
	Body      string    `json:"body"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type reviewDataBudget struct {
	limit int
	used  int
}

func (budget *reviewDataBudget) retain(size int) bool {
	if size > budget.limit-budget.used {
		return false
	}
	budget.used += size
	return true
}

func NewClient(token string) *Client {
	return &Client{
		token:       token,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
		restBaseURL: "https://api.github.com",
		graphQLURL:  "https://api.github.com/graphql",
	}
}

func AuthToken(ctx context.Context) (string, error) {
	for _, name := range tokenEnvironmentNames {
		if token := strings.TrimSpace(os.Getenv(name)); token != "" {
			return token, nil
		}
	}
	output, err := exec.CommandContext(ctx, "gh", "auth", "token", "--hostname", "github.com").Output()
	if err != nil {
		return "", fmt.Errorf("get GitHub token from gh: %w", err)
	}
	token := strings.TrimSpace(string(output))
	if token == "" {
		return "", fmt.Errorf("gh auth token returned an empty token")
	}
	return token, nil
}

func WithoutAuthTokens(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, value := range environment {
		name, _, _ := strings.Cut(value, "=")
		if !slices.Contains(tokenEnvironmentNames, name) {
			filtered = append(filtered, value)
		}
	}
	return filtered
}

func (client *Client) OpenPullRequests(ctx context.Context, repository string, authors []string) ([]PullRequest, error) {
	owner, name, ok := strings.Cut(repository, "/")
	if !ok {
		return nil, fmt.Errorf("invalid repository %q", repository)
	}
	authorSet := make(map[string]bool, len(authors))
	for _, author := range authors {
		authorSet[strings.ToLower(author)] = true
	}

	var pullRequests []PullRequest
	for page := 1; ; page++ {
		endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls?state=open&per_page=%d&page=%d", client.restBaseURL, url.PathEscape(owner), url.PathEscape(name), githubPageSize, page)
		var response []struct {
			Number  int    `json:"number"`
			Title   string `json:"title"`
			HTMLURL string `json:"html_url"`
			User    struct {
				Login string `json:"login"`
			} `json:"user"`
		}
		if err := client.get(ctx, endpoint, &response); err != nil {
			return nil, err
		}
		for _, pullRequest := range response {
			if len(authorSet) > 0 && !authorSet[strings.ToLower(pullRequest.User.Login)] {
				continue
			}
			pullRequests = append(pullRequests, PullRequest{
				Repository: repository,
				Number:     pullRequest.Number,
				Title:      pullRequest.Title,
				URL:        pullRequest.HTMLURL,
			})
		}
		if len(response) < githubPageSize {
			return pullRequests, nil
		}
	}
}

func (client *Client) ReviewThreads(ctx context.Context, pullRequest PullRequest) ([]ReviewThread, error) {
	return client.reviewThreads(ctx, pullRequest, maxReviewThreadDataBytes)
}

func (client *Client) reviewThreads(ctx context.Context, pullRequest PullRequest, dataLimit int) ([]ReviewThread, error) {
	owner, repository, ok := strings.Cut(pullRequest.Repository, "/")
	if !ok {
		return nil, fmt.Errorf("invalid repository %q", pullRequest.Repository)
	}

	var threads []ReviewThread
	budget := &reviewDataBudget{limit: dataLimit}
	var after *string
	for {
		var response reviewThreadsResponse
		if err := client.graphql(ctx, reviewThreadsQuery, map[string]any{
			"owner":         owner,
			"repo":          repository,
			"number":        pullRequest.Number,
			"first":         githubPageSize,
			"after":         after,
			"commentsFirst": githubPageSize,
		}, &response); err != nil {
			return nil, err
		}

		connection := response.Repository.PullRequest.ReviewThreads
		for _, node := range connection.Nodes {
			thread := threadFromNode(node)
			threadDataBytes := reviewThreadDataBytes(thread)
			if !budget.retain(threadDataBytes) {
				return nil, fmt.Errorf("review thread data exceeds %d bytes", dataLimit)
			}
			if node.Comments.PageInfo.HasNextPage {
				if node.Comments.PageInfo.EndCursor == "" {
					return nil, fmt.Errorf("review thread %s comments have another page without a cursor", node.ID)
				}
				comments, err := client.moreComments(ctx, node.ID, node.Comments.PageInfo.EndCursor, budget)
				if err != nil {
					return nil, err
				}
				thread.Comments = append(thread.Comments, comments...)
			}
			sortComments(thread.Comments)
			threads = append(threads, thread)
		}

		if !connection.PageInfo.HasNextPage {
			break
		}
		if connection.PageInfo.EndCursor == "" {
			return nil, fmt.Errorf("review threads have another page without a cursor")
		}
		after = &connection.PageInfo.EndCursor
	}
	sort.Slice(threads, func(left, right int) bool { return threads[left].ID < threads[right].ID })
	return threads, nil
}

func reviewThreadDataBytes(thread ReviewThread) int {
	size := len(thread.ID) + len(thread.Path) + len(thread.DiffSide) + len(thread.StartDiffSide)
	for _, comment := range thread.Comments {
		size += reviewCommentDataBytes(comment)
	}
	return size
}

func reviewCommentDataBytes(comment ReviewComment) int {
	return len(comment.ID) + len(comment.Author) + len(comment.Body) + len(comment.URL)
}

func (client *Client) moreComments(ctx context.Context, threadID, cursor string, budget *reviewDataBudget) ([]ReviewComment, error) {
	var comments []ReviewComment
	after := &cursor
	for {
		var response reviewThreadCommentsResponse
		if err := client.graphql(ctx, reviewThreadCommentsQuery, map[string]any{
			"id": threadID, "first": githubPageSize, "after": after,
		}, &response); err != nil {
			return nil, err
		}
		for _, node := range response.Node.Comments.Nodes {
			comment := commentFromNode(node)
			commentBytes := reviewCommentDataBytes(comment)
			if !budget.retain(commentBytes) {
				return nil, fmt.Errorf("review thread data exceeds %d bytes", budget.limit)
			}
			comments = append(comments, comment)
		}
		if !response.Node.Comments.PageInfo.HasNextPage {
			return comments, nil
		}
		if response.Node.Comments.PageInfo.EndCursor == "" {
			return nil, fmt.Errorf("review thread %s comments have another page without a cursor", threadID)
		}
		after = &response.Node.Comments.PageInfo.EndCursor
	}
}

func (client *Client) get(ctx context.Context, endpoint string, output any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	return client.do(request, output)
}

func (client *Client) graphql(ctx context.Context, query string, variables map[string]any, output any) error {
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.graphQLURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	var response struct {
		Data   json.RawMessage `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := client.do(request, &response); err != nil {
		return err
	}
	if len(response.Errors) > 0 {
		return fmt.Errorf("GitHub GraphQL error: %s", response.Errors[0].Message)
	}
	return json.Unmarshal(response.Data, output)
}

func (client *Client) do(request *http.Request, output any) error {
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", "bifrost")
	if client.token != "" {
		request.Header.Set("Authorization", "Bearer "+client.token)
	}
	if request.Method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxGitHubResponseBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxGitHubResponseBytes {
		return fmt.Errorf("GitHub API response exceeds %d bytes", maxGitHubResponseBytes)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("GitHub API %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	if output == nil {
		return nil
	}
	return json.Unmarshal(body, output)
}

type pageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type commentNode struct {
	ID     string `json:"id"`
	Author struct {
		Login string `json:"login"`
	} `json:"author"`
	BodyText  string    `json:"bodyText"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type commentsConnection struct {
	PageInfo pageInfo      `json:"pageInfo"`
	Nodes    []commentNode `json:"nodes"`
}

type threadNode struct {
	ID                string             `json:"id"`
	IsResolved        bool               `json:"isResolved"`
	IsOutdated        bool               `json:"isOutdated"`
	Path              string             `json:"path"`
	Line              *int               `json:"line"`
	StartLine         *int               `json:"startLine"`
	OriginalLine      *int               `json:"originalLine"`
	OriginalStartLine *int               `json:"originalStartLine"`
	DiffSide          string             `json:"diffSide"`
	StartDiffSide     string             `json:"startDiffSide"`
	Comments          commentsConnection `json:"comments"`
}

type reviewThreadsResponse struct {
	Repository struct {
		PullRequest struct {
			ReviewThreads struct {
				PageInfo pageInfo     `json:"pageInfo"`
				Nodes    []threadNode `json:"nodes"`
			} `json:"reviewThreads"`
		} `json:"pullRequest"`
	} `json:"repository"`
}

type reviewThreadCommentsResponse struct {
	Node struct {
		Comments commentsConnection `json:"comments"`
	} `json:"node"`
}

func threadFromNode(node threadNode) ReviewThread {
	comments := make([]ReviewComment, 0, len(node.Comments.Nodes))
	for _, comment := range node.Comments.Nodes {
		comments = append(comments, commentFromNode(comment))
	}
	return ReviewThread{
		ID:                node.ID,
		IsResolved:        node.IsResolved,
		IsOutdated:        node.IsOutdated,
		Path:              node.Path,
		Line:              node.Line,
		StartLine:         node.StartLine,
		OriginalLine:      node.OriginalLine,
		OriginalStartLine: node.OriginalStartLine,
		DiffSide:          node.DiffSide,
		StartDiffSide:     node.StartDiffSide,
		Comments:          comments,
	}
}

func commentFromNode(node commentNode) ReviewComment {
	return ReviewComment{
		ID: node.ID, Author: node.Author.Login, Body: node.BodyText, URL: node.URL,
		CreatedAt: node.CreatedAt, UpdatedAt: node.UpdatedAt,
	}
}

func sortComments(comments []ReviewComment) {
	sort.Slice(comments, func(left, right int) bool {
		if comments[left].CreatedAt.Equal(comments[right].CreatedAt) {
			return comments[left].ID < comments[right].ID
		}
		return comments[left].CreatedAt.Before(comments[right].CreatedAt)
	})
}
