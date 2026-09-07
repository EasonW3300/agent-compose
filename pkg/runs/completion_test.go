package runs_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/samber/do/v2"

	appconfig "agent-compose/pkg/config"
	"agent-compose/pkg/internal/testutil"
	domain "agent-compose/pkg/model"
	"agent-compose/pkg/runs"
	"agent-compose/pkg/storage/configstore"
)

func TestCompletionCleanupAction(t *testing.T) {
	tests := []struct {
		name    string
		policy  string
		has     bool
		created bool
		want    string
	}{
		{name: "no sandbox", policy: domain.ProjectRunCleanupStopOnCompletion, want: domain.ProjectRunCompletionActionNone},
		{name: "keep running", policy: domain.ProjectRunCleanupKeepRunning, has: true, created: true, want: domain.ProjectRunCompletionActionNone},
		{name: "stop", policy: domain.ProjectRunCleanupStopOnCompletion, has: true, created: true, want: domain.ProjectRunCompletionActionStop},
		{name: "remove created", policy: domain.ProjectRunCleanupRemoveOnCompletion, has: true, created: true, want: domain.ProjectRunCompletionActionRemove},
		{name: "remove reused degrades to stop", policy: domain.ProjectRunCleanupRemoveOnCompletion, has: true, want: domain.ProjectRunCompletionActionStop},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runs.CompletionCleanupAction(tt.policy, tt.has, tt.created); got != tt.want {
				t.Fatalf("CompletionCleanupAction() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCompletionManagerPersistsEventsBeforeCleanupAndRetriesFirstResult(t *testing.T) {
	ctx := context.Background()
	store := newCompletionTestStore(t)
	run := createCompletionTestRun(t, store, domain.ProjectRunRecord{
		RunID: "completion-run", ProjectID: "project-1", AgentName: "worker", AgentID: "agent-1",
		Status: domain.ProjectRunStatusRunning, SandboxID: "sandbox-1", CleanupPolicy: domain.ProjectRunCleanupStopOnCompletion,
	})
	sandboxes := completionSandboxStoreFake{sandbox: &domain.Sandbox{Summary: domain.SandboxSummary{ID: run.SandboxID, VMStatus: domain.VMStatusRunning}}}
	timeoutCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopper := &completionStopperFake{err: errors.New("stop unavailable"), onStop: cancel}
	manager := runs.NewCompletionManager(runs.CompletionManagerDeps{Store: store, Sandboxes: sandboxes, Lifecycle: stopper})

	staged, err := manager.Complete(timeoutCtx, runs.TransitionRequest{
		RunID: run.RunID, Status: domain.ProjectRunStatusSucceeded, Output: "exact output", ResultJSON: `{"ok":true}`,
		TerminalEvents: []domain.ProjectRunEventRecord{{ID: "final-agent-event", RunID: run.RunID, Kind: domain.ProjectRunEventKindAgentMessage, Text: "done"}},
	})
	if err != nil {
		t.Fatalf("Complete while cleanup failed: %v", err)
	}
	if staged.Status != domain.ProjectRunStatusRunning || staged.CleanupError == "" {
		t.Fatalf("staged run = %#v, want running with cleanup error", staged)
	}
	events, err := store.ListProjectRunEvents(ctx, run.RunID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != "final-agent-event" || events[0].Kind == domain.ProjectRunEventKindStatus {
		t.Fatalf("events before cleanup = %#v", events)
	}
	journal, err := store.GetProjectRunCompletion(ctx, run.RunID)
	if err != nil || journal.Attempt != 1 || journal.CleanupAction != domain.ProjectRunCompletionActionStop {
		t.Fatalf("journal = %#v, err=%v", journal, err)
	}

	stopper.err = nil
	if err := store.RecordProjectRunCompletionFailure(ctx, domain.ProjectRunCompletionFailure{
		RunID: run.RunID, Message: journal.LastError, Attempt: journal.Attempt, NextAttemptAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	completed, err := manager.Complete(ctx, runs.TransitionRequest{RunID: run.RunID, Status: domain.ProjectRunStatusCanceled, Error: "late cancellation"})
	if err != nil {
		t.Fatalf("Complete retry: %v", err)
	}
	if completed.Status != domain.ProjectRunStatusSucceeded || completed.Output != "exact output" || completed.ResultJSON != `{"ok":true}` || completed.CleanupError != "" {
		t.Fatalf("completed run = %#v", completed)
	}
	events, err = store.ListProjectRunEvents(ctx, run.RunID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[1].Kind != domain.ProjectRunEventKindStatus {
		t.Fatalf("events after cleanup = %#v", events)
	}
	if _, err := store.GetProjectRunCompletion(ctx, run.RunID); err == nil {
		t.Fatal("completion journal remained after terminal commit")
	}
}

func TestIntegrationCompletionManagerPersistsEventsBeforeCleanupAndRetriesFirstResult(t *testing.T) {
	TestCompletionManagerPersistsEventsBeforeCleanupAndRetriesFirstResult(t)
}

func TestE2ECompletionManagerPersistsEventsBeforeCleanupAndRetriesFirstResult(t *testing.T) {
	TestCompletionManagerPersistsEventsBeforeCleanupAndRetriesFirstResult(t)
}

func TestCompletionManagerPreservesConversationTerminalTimingByOrigin(t *testing.T) {
	ctx := context.Background()
	startedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	for _, origin := range []string{domain.ProjectRunStatusPending, domain.ProjectRunStatusRunning} {
		for _, terminal := range []string{domain.ProjectRunStatusTimedOut, domain.ProjectRunStatusInterrupted} {
			t.Run(origin+"_to_"+terminal, func(t *testing.T) {
				store := newCompletionTestStore(t)
				runStartedAt := time.Time{}
				if origin == domain.ProjectRunStatusRunning {
					runStartedAt = startedAt
				}
				run := createCompletionTestRun(t, store, domain.ProjectRunRecord{
					RunID: "completion-" + origin + "-" + terminal, ProjectID: "project-1", AgentName: "worker", AgentID: "agent-1",
					Status: origin, StartedAt: runStartedAt,
				})
				manager := runs.NewCompletionManager(runs.CompletionManagerDeps{Store: store})

				completed, err := manager.Complete(ctx, runs.TransitionRequest{RunID: run.RunID, Status: terminal})
				if err != nil {
					t.Fatalf("complete %s run as %s: %v", origin, terminal, err)
				}
				if completed.Status != terminal || completed.CompletedAt.IsZero() {
					t.Fatalf("completed run = %#v, want terminal %s with completion time", completed, terminal)
				}
				if origin == domain.ProjectRunStatusPending {
					if !completed.StartedAt.IsZero() || completed.DurationMs != 0 {
						t.Fatalf("completed pending run = %#v, want zero start time and duration", completed)
					}
					return
				}
				if !completed.StartedAt.Equal(startedAt) || completed.DurationMs <= 0 {
					t.Fatalf("completed running run = %#v, want start at %s with positive duration", completed, startedAt)
				}
			})
		}
	}
}

type completionSandboxStoreFake struct{ sandbox *domain.Sandbox }

func (s completionSandboxStoreFake) GetSandbox(context.Context, string) (*domain.Sandbox, error) {
	if s.sandbox == nil {
		return nil, domain.ErrNotFound
	}
	return s.sandbox, nil
}

type completionStopperFake struct {
	err    error
	calls  int
	onStop func()
}

func (s *completionStopperFake) Stop(context.Context, *domain.Sandbox) error {
	s.calls++
	if s.onStop != nil {
		s.onStop()
	}
	return s.err
}

func newCompletionTestStore(t *testing.T) *configstore.ConfigStore {
	t.Helper()
	root := t.TempDir()
	di := do.New()
	do.ProvideValue(di, context.Background())
	do.ProvideValue(di, &appconfig.Config{DataRoot: root, DbAddr: filepath.Join(root, "data.db")})
	store, err := testutil.OpenConfigStore(t, di)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.DB().Close() })
	if _, err := store.UpsertProject(context.Background(), domain.ProjectRecord{ID: "project-1", Name: "project", SourcePath: "/project", SourceJSON: `{}`}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertProjectAgent(context.Background(), domain.ProjectAgentRecord{ID: "agent-1", ProjectID: "project-1", AgentName: "worker"}); err != nil {
		t.Fatal(err)
	}
	return store
}

func createCompletionTestRun(t *testing.T, store *configstore.ConfigStore, run domain.ProjectRunRecord) domain.ProjectRunRecord {
	t.Helper()
	created, err := store.CreateProjectRun(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	return created
}
