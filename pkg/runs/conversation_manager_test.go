package runs

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	driverpkg "agent-compose/pkg/driver"
	domain "agent-compose/pkg/model"
)

func TestConversationSurvivesObserverDisconnectAndResumes(t *testing.T) {
	ctx := context.Background()
	controller, store, runtime := newConversationManagerTestController(t)
	interaction := newScriptedRunAttachInteraction()
	runtime.interactionOverride = interaction
	manager := NewConversationManager(ConversationManagerDeps{Root: ctx, Controller: controller})
	t.Cleanup(func() {
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown manager: %v", err)
		}
	})

	run, err := manager.Start(ctx, RunAgentRequest{ProjectID: "project-1", AgentName: "worker", Prompt: "find the log"})
	if err != nil {
		t.Fatalf("start conversation: %v", err)
	}
	if !projectRunIsInteractiveConversation(run) {
		t.Fatalf("interactive start stored run without the durable conversation marker: %#v", run)
	}
	assertPromptRuntimeFrame(t, receiveRuntimeInputFrame(t, interaction.sent), "start", "")
	assertPromptRuntimeFrame(t, receiveRuntimeInputFrame(t, interaction.sent), "human_message", "find the log")

	interaction.frames <- driverpkg.RuntimeOutputFrame{Type: driverpkg.RuntimeOutputStarted}
	interaction.frames <- promptRuntimeStdoutFrame(`{"v":1,"seq":1,"type":"agent_turn_completed","provider":"codex","turnState":"needs_input","finalText":"日志在哪？","finalTextSource":"provider_message"}`)
	assertConversationStatus(t, store, run.RunID, domain.ProjectRunStatusWaitingForInput)
	select {
	case <-interaction.closed:
		t.Fatal("runtime input closed while the conversation was waiting for a detached reply")
	default:
	}

	resumed, err := manager.Reply(ctx, run.RunID, "/workspace/log.md", "message-1")
	if err != nil {
		t.Fatalf("reply to conversation: %v", err)
	}
	if resumed.Status != domain.ProjectRunStatusRunning {
		t.Fatalf("reply run status = %q, want running", resumed.Status)
	}
	assertConversationStatus(t, store, run.RunID, domain.ProjectRunStatusRunning)
	assertPromptRuntimeFrame(t, receiveRuntimeInputFrame(t, interaction.sent), "human_message", "/workspace/log.md")
	duplicate, err := manager.Reply(ctx, run.RunID, "/workspace/log.md", "message-1")
	if err != nil {
		t.Fatalf("duplicate reply after resume: %v", err)
	}
	if duplicate.Status != domain.ProjectRunStatusRunning {
		t.Fatalf("duplicate reply run status = %q, want running", duplicate.Status)
	}
	select {
	case frame := <-interaction.sent:
		t.Fatalf("duplicate reply sent runtime frame %#v", frame)
	default:
	}

	interaction.frames <- promptRuntimeStdoutFrame(`{"v":1,"seq":2,"type":"agent_turn_completed","provider":"codex","turnState":"completed","finalText":"已整理","finalTextSource":"provider_message"}`)
	interaction.frames <- driverpkg.RuntimeOutputFrame{Type: driverpkg.RuntimeOutputResult, Result: &driverpkg.RuntimeResult{OperationID: run.RunID, Success: true}}
	assertConversationStatus(t, store, run.RunID, domain.ProjectRunStatusSucceeded)
	close(interaction.frames)
}

func TestConversationReplyRetryAfterCompletionReturnsDurableResult(t *testing.T) {
	ctx := context.Background()
	controller, store, runtime := newConversationManagerTestController(t)
	interaction := newScriptedRunAttachInteraction()
	runtime.interactionOverride = interaction
	manager := NewConversationManager(ConversationManagerDeps{Root: ctx, Controller: controller})
	t.Cleanup(func() {
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown manager: %v", err)
		}
	})

	run, err := manager.Start(ctx, RunAgentRequest{ProjectID: "project-1", AgentName: "worker", Prompt: "find the log"})
	if err != nil {
		t.Fatalf("start conversation: %v", err)
	}
	_ = receiveRuntimeInputFrame(t, interaction.sent)
	_ = receiveRuntimeInputFrame(t, interaction.sent)
	interaction.frames <- driverpkg.RuntimeOutputFrame{Type: driverpkg.RuntimeOutputStarted}
	interaction.frames <- promptRuntimeStdoutFrame(`{"v":1,"seq":1,"type":"agent_turn_completed","provider":"codex","turnState":"needs_input","finalText":"日志在哪？","finalTextSource":"provider_message"}`)
	assertConversationStatus(t, store, run.RunID, domain.ProjectRunStatusWaitingForInput)

	first, err := manager.Reply(ctx, run.RunID, "/workspace/log.md", "message-1")
	if err != nil {
		t.Fatalf("first reply: %v", err)
	}
	assertPromptRuntimeFrame(t, receiveRuntimeInputFrame(t, interaction.sent), "human_message", "/workspace/log.md")
	interaction.frames <- promptRuntimeStdoutFrame(`{"v":1,"seq":2,"type":"agent_turn_completed","provider":"codex","turnState":"completed","finalText":"已整理","finalTextSource":"provider_message"}`)
	interaction.frames <- driverpkg.RuntimeOutputFrame{Type: driverpkg.RuntimeOutputResult, Result: &driverpkg.RuntimeResult{OperationID: run.RunID, Success: true}}
	assertConversationStatus(t, store, run.RunID, domain.ProjectRunStatusSucceeded)
	waitForConversationUnregister(t, manager, run.RunID)

	retry, err := manager.Reply(ctx, run.RunID, "/workspace/log.md", "message-1")
	if err != nil {
		t.Fatalf("retry after completion: %v", err)
	}
	if retry.RunID != first.RunID || retry.Status != domain.ProjectRunStatusSucceeded {
		t.Fatalf("retry result = %#v, want completed original run %#v", retry, first)
	}
	select {
	case frame := <-interaction.sent:
		t.Fatalf("completed-conversation retry sent runtime frame %#v", frame)
	default:
	}
	close(interaction.frames)
}

func projectRunIsInteractiveConversation(run domain.ProjectRunRecord) bool {
	field := reflect.ValueOf(run).FieldByName("InteractiveConversation")
	return field.IsValid() && field.Kind() == reflect.Bool && field.Bool()
}

func waitForConversationUnregister(t *testing.T, manager *ConversationManager, runID string) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		manager.mu.Lock()
		_, active := manager.conversations[runID]
		manager.mu.Unlock()
		if !active {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("conversation %s remained registered after completion", runID)
		case <-time.After(time.Millisecond):
		}
	}
}

func assertConversationStatus(t *testing.T, store interface {
	GetProjectRun(context.Context, string) (domain.ProjectRunRecord, error)
}, runID, want string) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		run, err := store.GetProjectRun(context.Background(), runID)
		if err == nil && run.Status == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("run %s status = %q err=%v, want %q", runID, run.Status, err, want)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestConversationTimeoutMarksRunTimedOut(t *testing.T) {
	ctx := context.Background()
	controller, store, runtime := newConversationManagerTestController(t)
	interaction := newScriptedRunAttachInteraction()
	runtime.interactionOverride = interaction
	manager := NewConversationManager(ConversationManagerDeps{Root: ctx, Controller: controller, Timeout: 10 * time.Millisecond})
	t.Cleanup(func() {
		close(interaction.frames)
		if err := manager.Shutdown(context.Background()); err != nil {
			t.Fatalf("shutdown manager: %v", err)
		}
	})

	run, err := manager.Start(ctx, RunAgentRequest{ProjectID: "project-1", AgentName: "worker", Prompt: "wait"})
	if err != nil {
		t.Fatalf("start conversation: %v", err)
	}
	assertPromptRuntimeFrame(t, receiveRuntimeInputFrame(t, interaction.sent), "start", "")
	assertPromptRuntimeFrame(t, receiveRuntimeInputFrame(t, interaction.sent), "human_message", "wait")
	assertConversationStatus(t, store, run.RunID, domain.ProjectRunStatusTimedOut)
	assertPromptRuntimeFrame(t, receiveRuntimeInputFrame(t, interaction.sent), "cancel", "conversation timed out")
}

func TestConversationRecoveryMarksNonterminalRunsInterrupted(t *testing.T) {
	store := &conversationRecoveryStore{fakeControllerStore: &fakeControllerStore{runs: map[string]domain.ProjectRunRecord{
		"running": {RunID: "running", Status: domain.ProjectRunStatusRunning, Prompt: "continue", InteractiveConversation: true, CleanupPolicy: domain.ProjectRunCleanupKeepRunning},
		"waiting": {RunID: "waiting", Status: domain.ProjectRunStatusWaitingForInput, Prompt: "continue", InteractiveConversation: true, CleanupPolicy: domain.ProjectRunCleanupKeepRunning},
		"command": {RunID: "command", Status: domain.ProjectRunStatusRunning, CleanupPolicy: domain.ProjectRunCleanupKeepRunning},
	}}}
	manager := NewConversationManager(ConversationManagerDeps{Controller: &Controller{configDB: store}})
	if err := manager.RecoverInterrupted(context.Background()); err != nil {
		t.Fatalf("recover interrupted conversations: %v", err)
	}
	for _, runID := range []string{"running", "waiting"} {
		run, err := store.GetProjectRun(context.Background(), runID)
		if err != nil || run.Status != domain.ProjectRunStatusInterrupted {
			t.Fatalf("recovered run %s = %#v err=%v, want interrupted", runID, run, err)
		}
	}
	command, err := store.GetProjectRun(context.Background(), "command")
	if err != nil || command.Status != domain.ProjectRunStatusRunning {
		t.Fatalf("command run = %#v err=%v, want unchanged running command", command, err)
	}
}

func TestConversationRecoveryCollectsEveryPageBeforeChangingStatuses(t *testing.T) {
	const conversationsPerStatus = 201
	runs := map[string]domain.ProjectRunRecord{
		"running-command": {RunID: "running-command", Status: domain.ProjectRunStatusRunning, CleanupPolicy: domain.ProjectRunCleanupKeepRunning},
		"waiting-command": {RunID: "waiting-command", Status: domain.ProjectRunStatusWaitingForInput, CleanupPolicy: domain.ProjectRunCleanupKeepRunning},
		"terminal":        {RunID: "terminal", Status: domain.ProjectRunStatusSucceeded, Prompt: "already finished", InteractiveConversation: true, CleanupPolicy: domain.ProjectRunCleanupKeepRunning},
	}
	for _, status := range []string{domain.ProjectRunStatusRunning, domain.ProjectRunStatusWaitingForInput} {
		for index := 0; index < conversationsPerStatus; index++ {
			runID := fmt.Sprintf("%s-prompt-%03d", status, index)
			runs[runID] = domain.ProjectRunRecord{
				RunID: runID, Status: status, Prompt: "continue", InteractiveConversation: true, CleanupPolicy: domain.ProjectRunCleanupKeepRunning,
			}
		}
	}
	store := &conversationRecoveryStore{fakeControllerStore: &fakeControllerStore{runs: runs}}
	manager := NewConversationManager(ConversationManagerDeps{Controller: &Controller{configDB: store}})

	if err := manager.RecoverInterrupted(context.Background()); err != nil {
		t.Fatalf("recover interrupted conversations: %v", err)
	}
	for _, status := range []string{domain.ProjectRunStatusRunning, domain.ProjectRunStatusWaitingForInput} {
		for index := 0; index < conversationsPerStatus; index++ {
			runID := fmt.Sprintf("%s-prompt-%03d", status, index)
			run, err := store.GetProjectRun(context.Background(), runID)
			if err != nil || run.Status != domain.ProjectRunStatusInterrupted {
				t.Fatalf("recovered run %s = %#v err=%v, want interrupted", runID, run, err)
			}
		}
	}
	for _, runID := range []string{"running-command", "waiting-command"} {
		run, err := store.GetProjectRun(context.Background(), runID)
		if err != nil || run.Status == domain.ProjectRunStatusInterrupted {
			t.Fatalf("command run %s = %#v err=%v, want original nonterminal status", runID, run, err)
		}
	}
	terminal, err := store.GetProjectRun(context.Background(), "terminal")
	if err != nil || terminal.Status != domain.ProjectRunStatusSucceeded {
		t.Fatalf("terminal run = %#v err=%v, want unchanged succeeded", terminal, err)
	}
}

type conversationRecoveryStore struct {
	*fakeControllerStore
}

func (s *conversationRecoveryStore) ListProjectRunsByOptions(_ context.Context, options domain.ProjectRunListOptions) ([]domain.ProjectRunRecord, error) {
	items := make([]domain.ProjectRunRecord, 0, len(s.runs))
	for _, run := range s.runs {
		if options.Status == "" || run.Status == options.Status {
			items = append(items, run)
		}
	}
	sort.Slice(items, func(left, right int) bool {
		return items[left].RunID < items[right].RunID
	})
	offset := options.Offset
	if offset >= len(items) {
		return nil, nil
	}
	limit := options.Limit
	if limit <= 0 || offset+limit > len(items) {
		limit = len(items) - offset
	}
	return items[offset : offset+limit], nil
}

// synchronizedConversationStore keeps the existing reusable controller fake
// safe while a manager receive goroutine and a test assertion read the same
// durable run/event boundary concurrently.
type synchronizedConversationStore struct {
	*fakeControllerStore
	mu sync.Mutex
}

func newConversationManagerTestController(t *testing.T) (*Controller, *synchronizedConversationStore, *fakeRunAttachRuntime) {
	t.Helper()
	controller, base, runtime := newTestRunAttachController(t, nil)
	store := &synchronizedConversationStore{fakeControllerStore: base}
	controller.configDB = store
	return controller, store, runtime
}

func (s *synchronizedConversationStore) CreateProjectRun(ctx context.Context, run domain.ProjectRunRecord) (domain.ProjectRunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeControllerStore.CreateProjectRun(ctx, run)
}

func (s *synchronizedConversationStore) CreateProjectRunWithEvents(ctx context.Context, run domain.ProjectRunRecord, events []domain.ProjectRunEventRecord) (domain.ProjectRunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeControllerStore.CreateProjectRunWithEvents(ctx, run, events)
}

func (s *synchronizedConversationStore) GetProjectRun(ctx context.Context, runID string) (domain.ProjectRunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeControllerStore.GetProjectRun(ctx, runID)
}

func (s *synchronizedConversationStore) UpdateProjectRun(ctx context.Context, run domain.ProjectRunRecord) (domain.ProjectRunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeControllerStore.UpdateProjectRun(ctx, run)
}

func (s *synchronizedConversationStore) UpdateProjectRunWithEvents(ctx context.Context, run domain.ProjectRunRecord, events []domain.ProjectRunEventRecord) (domain.ProjectRunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeControllerStore.UpdateProjectRunWithEvents(ctx, run, events)
}

func (s *synchronizedConversationStore) AppendProjectRunEvent(ctx context.Context, event domain.ProjectRunEventRecord) (domain.ProjectRunEventRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeControllerStore.AppendProjectRunEvent(ctx, event)
}

func (s *synchronizedConversationStore) AppendProjectRunEvents(ctx context.Context, events []domain.ProjectRunEventRecord) ([]domain.ProjectRunEventRecord, []bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeControllerStore.AppendProjectRunEvents(ctx, events)
}

func (s *synchronizedConversationStore) ListProjectRunEvents(_ context.Context, runID string, afterSequence uint64, limit int) ([]domain.ProjectRunEventRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]domain.ProjectRunEventRecord, 0)
	for _, event := range s.fakeControllerStore.events {
		if event.RunID == runID && event.Sequence > afterSequence {
			items = append(items, event)
		}
	}
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}
