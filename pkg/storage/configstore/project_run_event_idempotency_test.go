package configstore

import (
	"context"
	"testing"

	domain "agent-compose/pkg/model"
)

// domain supplies the persisted run and event records exercised against the
// SQLite event transaction, including its message-ID idempotency boundary.

func TestProjectRunEventsDeduplicateUserMessageClientMessageID(t *testing.T) {
	ctx := context.Background()
	store := FromDB(newMemoryDB(t))
	if err := store.initSchema(ctx); err != nil {
		t.Fatalf("init schema: %v", err)
	}
	project, err := store.UpsertProject(ctx, domain.ProjectRecord{ID: "project-client-message", Name: "client-message"})
	if err != nil {
		t.Fatalf("create project: %v", err)
	}
	agentID := createRunEventTestAgent(t, runEventTestAgentSpec{Ctx: ctx, Store: store, ProjectID: project.ID, AgentName: "worker"})
	if _, err := store.CreateProjectRun(ctx, domain.ProjectRunRecord{RunID: "run-client-message", ProjectID: project.ID, AgentName: "worker", AgentID: agentID, Status: domain.ProjectRunStatusRunning}); err != nil {
		t.Fatalf("create run: %v", err)
	}

	first, created, err := store.AppendProjectRunEvent(ctx, domain.ProjectRunEventRecord{
		ID: "message-event-one", RunID: "run-client-message", Kind: domain.ProjectRunEventKindUserMessage, Text: "first submission", PayloadJSON: `{"client_message_id":"message-1"}`,
	})
	if err != nil || !created {
		t.Fatalf("append first message = %#v, created=%v, err=%v", first, created, err)
	}
	retry, created, err := store.AppendProjectRunEvent(ctx, domain.ProjectRunEventRecord{
		ID: "message-event-retry", RunID: "run-client-message", Kind: domain.ProjectRunEventKindUserMessage, Text: "retry submission", PayloadJSON: `{"client_message_id":"message-1"}`,
	})
	if err != nil || created || retry.ID != first.ID || retry.Text != first.Text {
		t.Fatalf("append retry = %#v, created=%v, err=%v", retry, created, err)
	}
	events, err := store.ListProjectRunEvents(ctx, "run-client-message", 0, 10)
	if err != nil || len(events) != 1 || events[0].PayloadJSON != `{"client_message_id":"message-1"}` {
		t.Fatalf("stored message events = %#v, err=%v", events, err)
	}
}
