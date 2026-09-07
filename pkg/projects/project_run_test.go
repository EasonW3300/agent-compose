package projects

import (
	"testing"

	domain "agent-compose/pkg/model"
)

// domain defines the persisted run statuses that project read/query helpers
// must preserve instead of silently presenting them as newly started runs.

func TestProjectRunNormalizationPreservesConversationStatuses(t *testing.T) {
	t.Parallel()

	for _, status := range []string{
		domain.ProjectRunStatusWaitingForInput,
		domain.ProjectRunStatusTimedOut,
		domain.ProjectRunStatusInterrupted,
	} {
		t.Run(status, func(t *testing.T) {
			if got := NormalizeRunStatus(status); got != status {
				t.Fatalf("NormalizeRunStatus(%q) = %q, want %q", status, got, status)
			}
			filtered := NormalizeRunStatusFilter([]string{status})
			if len(filtered) != 1 || filtered[0] != status {
				t.Fatalf("NormalizeRunStatusFilter(%q) = %#v", status, filtered)
			}
		})
	}
}
