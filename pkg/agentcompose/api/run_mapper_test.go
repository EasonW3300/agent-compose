package api

import (
	"testing"
	"time"

	domain "agent-compose/pkg/model"
	agentcomposev2 "agent-compose/proto/agentcompose/v2"
)

// domain supplies the persisted run-status values that these boundary mappings
// translate to and from the generated protobuf enum.

func TestFormatProjectTimePreservesInstantAndNanosecondPrecision(t *testing.T) {
	t.Parallel()
	zone := time.FixedZone("UTC+08:30", 8*60*60+30*60)
	input := time.Date(2026, 7, 23, 12, 34, 56, 123456789, zone)

	got := FormatProjectTime(input)
	if err := got.CheckValid(); err != nil {
		t.Fatalf("FormatProjectTime() returned invalid timestamp: %v", err)
	}
	if !got.AsTime().Equal(input) {
		t.Fatalf("FormatProjectTime() = %s, want instant %s", got.AsTime(), input)
	}
	if got.AsTime().Nanosecond() != input.Nanosecond() {
		t.Fatalf("nanoseconds = %d, want %d", got.AsTime().Nanosecond(), input.Nanosecond())
	}
}

func TestFormatProjectTimeOmitsZeroValue(t *testing.T) {
	t.Parallel()
	if got := FormatProjectTime(time.Time{}); got != nil {
		t.Fatalf("FormatProjectTime(zero) = %v, want nil", got)
	}
}

func TestProjectRunStatusFromProtoWaitsForInput(t *testing.T) {
	t.Parallel()

	if got := ProjectRunStatusFromProto(agentcomposev2.RunStatus_RUN_STATUS_WAITING_FOR_INPUT); got != domain.ProjectRunStatusWaitingForInput {
		t.Fatalf("status = %q", got)
	}
}

func TestProjectRunStatusToProtoWaitsForInput(t *testing.T) {
	t.Parallel()

	if got := ProjectRunStatusToProto(domain.ProjectRunStatusWaitingForInput); got != agentcomposev2.RunStatus_RUN_STATUS_WAITING_FOR_INPUT {
		t.Fatalf("status = %v", got)
	}
}

func TestProjectRunConversationStatusesMapBothDirections(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		domain string
		proto  agentcomposev2.RunStatus
	}{
		{"waiting for input", domain.ProjectRunStatusWaitingForInput, agentcomposev2.RunStatus_RUN_STATUS_WAITING_FOR_INPUT},
		{"timed out", domain.ProjectRunStatusTimedOut, agentcomposev2.RunStatus_RUN_STATUS_TIMED_OUT},
		{"interrupted", domain.ProjectRunStatusInterrupted, agentcomposev2.RunStatus_RUN_STATUS_INTERRUPTED},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := ProjectRunStatusToProto(test.domain); got != test.proto {
				t.Fatalf("ProjectRunStatusToProto(%q) = %v, want %v", test.domain, got, test.proto)
			}
			if got := ProjectRunStatusFromProto(test.proto); got != test.domain {
				t.Fatalf("ProjectRunStatusFromProto(%v) = %q, want %q", test.proto, got, test.domain)
			}
		})
	}
}
