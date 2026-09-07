package schedulers_test

import (
	"context"
	"errors"
	"testing"

	domain "agent-compose/pkg/model"
	"agent-compose/pkg/schedulers"
)

func TestSchedulerSkipsActiveConversation(t *testing.T) {
	runs := &activeConversationRunLister{runs: []domain.ProjectRunRecord{{
		RunID: "conversation-1", ProjectID: "project-1", AgentName: "reviewer", Prompt: "ask the user", Status: domain.ProjectRunStatusWaitingForInput,
	}}}
	runner := &hostProjectAgentRunnerFake{}
	events := &hostEventsFake{}
	host := schedulers.NewRuntimeHost(schedulers.RunHostDependencies{
		ProjectRuns:        runs,
		Events:             events,
		ProjectAgentRunner: runner,
	}, domain.Scheduler{Summary: domain.SchedulerSummary{ID: "scheduler-1", ProjectID: "project-1", AgentName: "reviewer"}}, schedulers.RuntimeExecutionContext{ID: "scheduler-run-1", Kind: schedulers.ExecutionKindTrigger}, schedulers.TriggerEventMetadata{})

	_, err := host.Agent(context.Background(), "scheduled prompt", domain.SchedulerAgentRequest{})
	if !errors.Is(err, schedulers.ErrActiveConversation) {
		t.Fatalf("Agent error = %v, want ErrActiveConversation", err)
	}
	if len(runner.requests) != 0 {
		t.Fatalf("project agent runner called %d times, want 0", len(runner.requests))
	}
	if !events.contains("scheduler.skipped_active_conversation") {
		t.Fatalf("scheduler events = %#v, want active conversation skip", events.types())
	}
}

type activeConversationRunLister struct {
	runs []domain.ProjectRunRecord
}

func (l *activeConversationRunLister) ListProjectRunsByOptions(context.Context, domain.ProjectRunListOptions) ([]domain.ProjectRunRecord, error) {
	return l.runs, nil
}
