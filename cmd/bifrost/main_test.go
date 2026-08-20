package main

import (
	"strings"
	"testing"

	"github.com/matan-yadgar/bifrost/internal/bridge"
)

func TestIncompleteDeliveryError(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name   string
		result bridge.CycleResult
		want   string
	}{
		{name: "complete"},
		{name: "pull requests deferred", result: bridge.CycleResult{Deferred: 2}, want: "2 pull requests"},
		{name: "threads deferred", result: bridge.CycleResult{DeferredThreads: 3}, want: "3 review threads"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := incompleteDeliveryError(testCase.result)
			if testCase.want == "" && err != nil {
				t.Fatal(err)
			}
			if testCase.want != "" && (err == nil || !strings.Contains(err.Error(), testCase.want)) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
