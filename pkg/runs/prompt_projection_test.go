package runs

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	domain "agent-compose/pkg/model"
)

// These tests exercise the projector's explicit runtime turn-state boundary and
// the coordinator transaction that makes projected conversation state durable.

func TestProjectorNeedsInputKeepsRunNonTerminal(t *testing.T) {
	t.Parallel()

	transition := projectConversationTurn(t, `{"type":"agent_turn_completed","turnState":"needs_input","finalText":"日志在哪？","finalTextSource":"provider_message"}`)
	if transition.Status != domain.ProjectRunStatusWaitingForInput {
		t.Fatalf("transition status = %q, want waiting_for_input", transition.Status)
	}
	if len(transition.TerminalEvents) != 1 || transition.TerminalEvents[0].Kind != domain.ProjectRunEventKindAgentMessage || transition.TerminalEvents[0].Text != "日志在哪？" {
		t.Fatalf("transition events = %#v", transition.TerminalEvents)
	}
}

func TestProjectorCompletedCreatesSucceededTransition(t *testing.T) {
	t.Parallel()

	transition := projectConversationTurn(t, `{"type":"agent_turn_completed","turnState":"completed","finalText":"已完成。","finalTextSource":"provider_message"}`)
	if transition.Status != domain.ProjectRunStatusSucceeded {
		t.Fatalf("transition status = %q, want succeeded", transition.Status)
	}
	if len(transition.TerminalEvents) != 1 || transition.TerminalEvents[0].Kind != domain.ProjectRunEventKindAgentMessage || transition.TerminalEvents[0].Text != "已完成。" {
		t.Fatalf("transition events = %#v", transition.TerminalEvents)
	}
}

func TestProjectorRejectsUnknownTurnState(t *testing.T) {
	t.Parallel()

	projector := newPersistentPromptAttachProjector(context.Background(), persistentPromptAttachProjectorDeps{
		Run:      domain.ProjectRunRecord{RunID: "run-projection", AgentName: "worker"},
		Sandbox:  &domain.Sandbox{},
		LogsPath: filepath.Join(t.TempDir(), "transcript.txt"),
	})
	if _, _, err := projector.Project([]byte(`{"type":"agent_turn_completed","turnState":"question","finalText":"Can you help?"}` + "\n")); err == nil {
		t.Fatal("unknown turn state did not return an error")
	}
}

func TestCoordinatorWaitingForInputPersistsAgentAndStatusEventsAtomically(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	store := &fakeRunStore{runs: map[string]domain.ProjectRunRecord{
		"run-waiting": {RunID: "run-waiting", Status: domain.ProjectRunStatusRunning, StartedAt: now.Add(-time.Minute)},
	}}
	coordinator := NewCoordinator(store, nil)
	coordinator.SetNow(func() time.Time { return now })

	updated, err := coordinator.TransitionRun(context.Background(), TransitionRequest{
		RunID:  "run-waiting",
		Status: domain.ProjectRunStatusWaitingForInput,
		TerminalEvents: []domain.ProjectRunEventRecord{{
			ID: "agent-question", RunID: "run-waiting", Kind: domain.ProjectRunEventKindAgentMessage, Text: "日志在哪？",
		}},
	})
	if err != nil {
		t.Fatalf("transition waiting run: %v", err)
	}
	if updated.Status != domain.ProjectRunStatusWaitingForInput || !updated.CompletedAt.IsZero() {
		t.Fatalf("updated run = %#v, want nonterminal waiting state", updated)
	}
	if len(store.events) != 2 || store.events[0].Kind != domain.ProjectRunEventKindAgentMessage || store.events[1].Kind != domain.ProjectRunEventKindStatus {
		t.Fatalf("stored events = %#v, want agent message followed by status", store.events)
	}
}

func TestCoordinatorPendingToRunningPersistsStatusEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	store := &fakeRunStore{runs: map[string]domain.ProjectRunRecord{
		"run-starting": {RunID: "run-starting", Status: domain.ProjectRunStatusPending},
	}}
	coordinator := NewCoordinator(store, nil)
	coordinator.SetNow(func() time.Time { return now })

	updated, err := coordinator.MarkRunning(context.Background(), "run-starting", "sandbox-1")
	if err != nil {
		t.Fatalf("mark running: %v", err)
	}
	if updated.Status != domain.ProjectRunStatusRunning || !updated.StartedAt.Equal(now) {
		t.Fatalf("updated run = %#v, want running at %s", updated, now)
	}
	if len(store.events) != 1 || store.events[0].Kind != domain.ProjectRunEventKindStatus {
		t.Fatalf("stored events = %#v, want running status event", store.events)
	}
}

func TestCoordinatorWaitingForInputCanResumeRunning(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	store := &fakeRunStore{runs: map[string]domain.ProjectRunRecord{
		"run-resuming": {RunID: "run-resuming", Status: domain.ProjectRunStatusWaitingForInput, StartedAt: now.Add(-time.Minute)},
	}}
	coordinator := NewCoordinator(store, nil)
	coordinator.SetNow(func() time.Time { return now })

	updated, err := coordinator.MarkRunning(context.Background(), "run-resuming", "sandbox-1")
	if err != nil {
		t.Fatalf("resume running: %v", err)
	}
	if updated.Status != domain.ProjectRunStatusRunning || !updated.CompletedAt.IsZero() {
		t.Fatalf("updated run = %#v, want resumed nonterminal running state", updated)
	}
	if len(store.events) != 1 || store.events[0].Kind != domain.ProjectRunEventKindStatus {
		t.Fatalf("stored events = %#v, want resume status event", store.events)
	}
}

func TestCoordinatorConversationTerminalStatusesCannotRestart(t *testing.T) {
	t.Parallel()

	for _, status := range []string{domain.ProjectRunStatusTimedOut, domain.ProjectRunStatusInterrupted} {
		t.Run(status, func(t *testing.T) {
			store := &fakeRunStore{runs: map[string]domain.ProjectRunRecord{
				"run-terminal": {RunID: "run-terminal", Status: status},
			}}
			coordinator := NewCoordinator(store, nil)
			if _, err := coordinator.MarkRunning(context.Background(), "run-terminal", "sandbox-1"); err == nil {
				t.Fatalf("terminal %s run restarted without an error", status)
			}
		})
	}
}

func TestCoordinatorCompletedTurnSetsCompletionTime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 7, 10, 0, 0, 0, time.UTC)
	store := &fakeRunStore{runs: map[string]domain.ProjectRunRecord{
		"run-completed": {RunID: "run-completed", Status: domain.ProjectRunStatusRunning, StartedAt: now.Add(-time.Minute)},
	}}
	coordinator := NewCoordinator(store, nil)
	coordinator.SetNow(func() time.Time { return now })

	updated, err := coordinator.TransitionRun(context.Background(), TransitionRequest{RunID: "run-completed", Status: domain.ProjectRunStatusSucceeded})
	if err != nil {
		t.Fatalf("transition completed run: %v", err)
	}
	if updated.Status != domain.ProjectRunStatusSucceeded || !updated.CompletedAt.Equal(now) {
		t.Fatalf("updated run = %#v, want completed success", updated)
	}
	if len(store.events) != 1 || store.events[0].Kind != domain.ProjectRunEventKindStatus {
		t.Fatalf("stored events = %#v, want terminal status", store.events)
	}
}

func projectConversationTurn(t *testing.T, frame string) TransitionRequest {
	t.Helper()
	projector := newPersistentPromptAttachProjector(context.Background(), persistentPromptAttachProjectorDeps{
		Run:        domain.ProjectRunRecord{RunID: "run-projection", AgentName: "worker"},
		Sandbox:    &domain.Sandbox{},
		LogsPath:   filepath.Join(t.TempDir(), "transcript.txt"),
		EventStore: &projectorEventStore{keys: map[string]struct{}{}},
	})
	_, transition, err := projector.Project([]byte(frame + "\n"))
	if err != nil {
		t.Fatalf("project frame: %v", err)
	}
	if transition == nil {
		t.Fatal("projected transition is nil")
	}
	return *transition
}
