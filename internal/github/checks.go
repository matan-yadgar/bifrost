package github

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

type FailedCheck struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	URL        string `json:"url"`
	Conclusion string `json:"conclusion"`
}

func (client *Client) FailedChecks(ctx context.Context, pullRequest PullRequest) ([]FailedCheck, error) {
	if pullRequest.HeadSHA == "" {
		return nil, fmt.Errorf("missing head SHA for %s", pullRequest.URL)
	}
	base := fmt.Sprintf("%s/repos/%s/commits/%s", client.restBaseURL, escapeRepository(pullRequest.Repository), url.PathEscape(pullRequest.HeadSHA))
	var failures []FailedCheck
	for page := 1; ; page++ {
		var response struct {
			CheckRuns []struct {
				ID          int64  `json:"id"`
				Name        string `json:"name"`
				Status      string `json:"status"`
				Conclusion  string `json:"conclusion"`
				CompletedAt string `json:"completed_at"`
				HTMLURL     string `json:"html_url"`
			} `json:"check_runs"`
		}
		if err := client.get(ctx, fmt.Sprintf("%s/check-runs?filter=latest&per_page=%d&page=%d", base, githubPageSize, page), &response); err != nil {
			return nil, err
		}
		for _, check := range response.CheckRuns {
			if check.Status == "completed" && failedConclusion(check.Conclusion) {
				failures = append(failures, FailedCheck{ID: fmt.Sprintf("check:%d:%s", check.ID, check.CompletedAt), Name: check.Name, URL: check.HTMLURL, Conclusion: check.Conclusion})
			}
		}
		if len(response.CheckRuns) < githubPageSize {
			break
		}
	}
	seen := make(map[string]bool)
	for page := 1; ; page++ {
		var response []struct {
			ID        int64  `json:"id"`
			Context   string `json:"context"`
			State     string `json:"state"`
			TargetURL string `json:"target_url"`
		}
		if err := client.get(ctx, fmt.Sprintf("%s/statuses?per_page=%d&page=%d", base, githubPageSize, page), &response); err != nil {
			return nil, err
		}
		for _, status := range response {
			if seen[status.Context] {
				continue
			}
			seen[status.Context] = true
			if status.State == "failure" || status.State == "error" {
				failures = append(failures, FailedCheck{ID: fmt.Sprintf("status:%d", status.ID), Name: status.Context, URL: status.TargetURL, Conclusion: status.State})
			}
		}
		if len(response) < githubPageSize {
			break
		}
	}
	return failures, nil
}

func failedConclusion(conclusion string) bool {
	switch conclusion {
	case "failure", "timed_out", "action_required", "startup_failure":
		return true
	}
	return false
}

func escapeRepository(repository string) string {
	parts := strings.Split(repository, "/")
	for index := range parts {
		parts[index] = url.PathEscape(parts[index])
	}
	return strings.Join(parts, "/")
}
