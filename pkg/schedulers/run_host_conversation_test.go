package schedulers_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	domain "agent-compose/pkg/model"
	"agent-compose/pkg/schedulers"
)

func TestSchedulerSkipsActiveConversation(t *testing.T) {
	runs := &activeConversationRunLister{runs: []domain.ProjectRunRecord{{
		RunID: "conversation-1", ProjectID: "project-1", AgentName: "reviewer", Prompt: "ask the user", Status: domain.ProjectRunStatusWaitingForInput,
	}}}
	markInteractiveConversation(&runs.runs[0])
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

func TestSchedulerOnlySkipsDurableInteractiveConversations(t *testing.T) {
	for _, test := range []struct {
		name     string
		runs     []domain.ProjectRunRecord
		wantSkip bool
	}{
		{name: "ordinary running prompt", runs: []domain.ProjectRunRecord{{RunID: "single-turn", ProjectID: "project-1", AgentName: "reviewer", Prompt: "run once", Status: domain.ProjectRunStatusRunning}}},
		{name: "interactive running prompt", runs: []domain.ProjectRunRecord{interactiveConversationRun("interactive-running", domain.ProjectRunStatusRunning)}, wantSkip: true},
		{name: "interactive waiting prompt", runs: []domain.ProjectRunRecord{interactiveConversationRun("interactive-waiting", domain.ProjectRunStatusWaitingForInput)}, wantSkip: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runs := &activeConversationRunLister{runs: test.runs}
			runner := &hostProjectAgentRunnerFake{}
			host := schedulers.NewRuntimeHost(schedulers.RunHostDependencies{ProjectRuns: runs, ProjectAgentRunner: runner}, domain.Scheduler{Summary: domain.SchedulerSummary{ID: "scheduler-1", ProjectID: "project-1", AgentName: "reviewer"}}, schedulers.RuntimeExecutionContext{ID: "scheduler-run-1", Kind: schedulers.ExecutionKindTrigger}, schedulers.TriggerEventMetadata{})

			_, err := host.Agent(context.Background(), "scheduled prompt", domain.SchedulerAgentRequest{})
			if test.wantSkip {
				if !errors.Is(err, schedulers.ErrActiveConversation) || len(runner.requests) != 0 {
					t.Fatalf("Agent err=%v calls=%d, want active-conversation skip", err, len(runner.requests))
				}
				return
			}
			if err != nil || len(runner.requests) != 1 {
				t.Fatalf("Agent err=%v calls=%d, want ordinary prompt execution", err, len(runner.requests))
			}
		})
	}
}

func interactiveConversationRun(runID, status string) domain.ProjectRunRecord {
	run := domain.ProjectRunRecord{RunID: runID, ProjectID: "project-1", AgentName: "reviewer", Prompt: "ask the user", Status: status}
	markInteractiveConversation(&run)
	return run
}

func markInteractiveConversation(run *domain.ProjectRunRecord) {
	field := reflect.ValueOf(run).Elem().FieldByName("InteractiveConversation")
	if field.IsValid() && field.CanSet() && field.Kind() == reflect.Bool {
		field.SetBool(true)
	}
}

type activeConversationRunLister struct {
	runs []domain.ProjectRunRecord
}

func (l *activeConversationRunLister) ListProjectRunsByOptions(context.Context, domain.ProjectRunListOptions) ([]domain.ProjectRunRecord, error) {
	return l.runs, nil
}
