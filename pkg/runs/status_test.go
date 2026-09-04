package runs

import (
	"testing"

	domain "agent-compose/pkg/model"
)

// domain supplies persisted status values accepted by NormalizeStatus at the
// run-lifecycle boundary.

func TestNormalizeStatusPreservesConversationStatuses(t *testing.T) {
	t.Parallel()

	for _, status := range []string{
		domain.ProjectRunStatusWaitingForInput,
		domain.ProjectRunStatusTimedOut,
		domain.ProjectRunStatusInterrupted,
	} {
		if got := NormalizeStatus(status); got != status {
			t.Fatalf("NormalizeStatus(%q) = %q, want %q", status, got, status)
		}
	}
}
