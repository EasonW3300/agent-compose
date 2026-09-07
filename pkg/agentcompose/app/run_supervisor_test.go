package app

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/samber/do/v2"

	appconfig "agent-compose/pkg/config"
	"agent-compose/pkg/internal/testutil"
	domain "agent-compose/pkg/model"
	"agent-compose/pkg/runs"
	"agent-compose/pkg/storage/configstore"
)

func TestRunSupervisorStopActiveRunRequestsCancellationWithoutMarkingTerminal(t *testing.T) {
	ctx := context.Background()
	store := newRunSupervisorTestConfigStore(t)
	if _, err := store.UpsertProject(ctx, domain.ProjectRecord{
		ID:         "project-1",
		Name:       "project",
		SourcePath: "/tmp/project",
		SourceJSON: "{}",
	}); err != nil {
		t.Fatalf("UpsertProject returned error: %v", err)
	}
	agent, err := store.UpsertProjectAgent(ctx, domain.ProjectAgentRecord{ProjectID: "project-1", AgentName: "worker"})
	if err != nil {
		t.Fatalf("UpsertProjectAgent returned error: %v", err)
	}
	if _, err := store.CreateProjectRun(ctx, domain.ProjectRunRecord{
		RunID:       "run-1",
		ProjectID:   "project-1",
		ProjectName: "project",
		AgentName:   "worker",
		AgentID:     agent.ID,
		Source:      domain.ProjectRunSourceManual,
		Status:      domain.ProjectRunStatusRunning,
		ResultJSON:  "{}",
	}); err != nil {
		t.Fatalf("CreateProjectRun returned error: %v", err)
	}

	cancelCalls := 0
	var cancelCause error
	supervisor := &RunSupervisor{
		store:  store,
		active: map[string]*activeRun{"run-1": {cancel: func(cause error) { cancelCalls++; cancelCause = cause }}},
	}
	stopped, err := supervisor.StopActiveRun(ctx, "run-1", "user stop")
	if err != nil {
		t.Fatalf("StopActiveRun returned error: %v", err)
	}
	if !stopped || cancelCalls != 1 || cancelCause == nil || cancelCause.Error() != "user stop" {
		t.Fatalf("first stop stopped=%v cancelCalls=%d, want true/1", stopped, cancelCalls)
	}
	if _, ok := supervisor.active["run-1"]; !ok {
		t.Fatalf("run was removed before execution exited")
	}

	stopped, err = supervisor.StopActiveRun(ctx, "run-1", "second stop")
	if err != nil {
		t.Fatalf("second StopActiveRun returned error: %v", err)
	}
	if !stopped || cancelCalls != 1 {
		t.Fatalf("second stop stopped=%v cancelCalls=%d, want true/1", stopped, cancelCalls)
	}
	run, err := store.GetProjectRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("GetProjectRun returned error: %v", err)
	}
	if run.Status != domain.ProjectRunStatusRunning || run.Error != "" {
		t.Fatalf("run after stop = %#v", run)
	}
	supervisor.unregister("run-1")
	if _, ok := supervisor.active["run-1"]; ok {
		t.Fatal("run remained active after execution exit")
	}
}

func TestRunSupervisorUnregisterRemovesStoppingRun(t *testing.T) {
	active := &activeRun{
		cancel:   func(error) {},
		stopping: true,
	}
	supervisor := &RunSupervisor{
		active: map[string]*activeRun{"run-1": active},
	}
	supervisor.unregister("run-1")
	if _, ok := supervisor.active["run-1"]; ok {
		t.Fatalf("stopping run remained registered")
	}
}

func TestNewRunSupervisorRecoversInterruptedConversations(t *testing.T) {
	ctx := context.Background()
	store := newRunSupervisorTestConfigStore(t)
	if _, err := store.UpsertProject(ctx, domain.ProjectRecord{
		ID: "project-1", Name: "project", SourcePath: "/tmp/project", SourceJSON: "{}",
	}); err != nil {
		t.Fatalf("UpsertProject returned error: %v", err)
	}
	agent, err := store.UpsertProjectAgent(ctx, domain.ProjectAgentRecord{ProjectID: "project-1", AgentName: "worker"})
	if err != nil {
		t.Fatalf("UpsertProjectAgent returned error: %v", err)
	}
	for _, status := range []string{domain.ProjectRunStatusRunning, domain.ProjectRunStatusWaitingForInput} {
		if _, err := store.CreateProjectRun(ctx, domain.ProjectRunRecord{
			RunID: "run-" + status, ProjectID: "project-1", ProjectName: "project", AgentName: "worker", AgentID: agent.ID,
			Source: domain.ProjectRunSourceManual, Status: status, Prompt: "continue", InteractiveConversation: true, CleanupPolicy: domain.ProjectRunCleanupKeepRunning, ResultJSON: "{}",
		}); err != nil {
			t.Fatalf("CreateProjectRun(%s) returned error: %v", status, err)
		}
	}
	di := do.New()
	do.ProvideValue(di, ctx)
	do.ProvideValue(di, runs.NewController(runs.ControllerDependencies{ConfigDB: store}))
	do.ProvideValue(di, store)
	if _, err := NewRunSupervisor(di); err != nil {
		t.Fatalf("NewRunSupervisor returned error: %v", err)
	}
	for _, runID := range []string{"run-running", "run-waiting_for_input"} {
		run, err := store.GetProjectRun(ctx, runID)
		if err != nil || run.Status != domain.ProjectRunStatusInterrupted {
			t.Fatalf("recovered run %s = %#v err=%v, want interrupted", runID, run, err)
		}
	}
}

func newRunSupervisorTestConfigStore(t *testing.T) *configstore.ConfigStore {
	t.Helper()
	root := t.TempDir()
	di := do.New()
	do.ProvideValue(di, context.Background())
	do.ProvideValue(di, &appconfig.Config{
		DataRoot: root,
		DbAddr:   filepath.Join(root, "data.db"),
	})
	store, err := testutil.OpenConfigStore(t, di)
	if err != nil {
		t.Fatalf("NewConfigStore returned error: %v", err)
	}
	t.Cleanup(func() {
		if db := store.DB(); db != nil {
			_ = db.Close()
		}
	})
	return store
}
