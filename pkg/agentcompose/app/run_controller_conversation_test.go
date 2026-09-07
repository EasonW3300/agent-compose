package app

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"

	"agent-compose/pkg/agentcompose/api"
	domain "agent-compose/pkg/model"
	"agent-compose/pkg/runs"
	agentcomposev2 "agent-compose/proto/agentcompose/v2"
)

func TestSendRunHumanMessageRejectsNonWaitingRun(t *testing.T) {
	delegate := &conversationRunDelegateFake{replyErr: domain.ResourceError(
		domain.ErrFailedPrecondition, "project run", "done", "project run done is not waiting for input", nil,
	)}
	handler := api.NewRunHandler(delegate, nil)

	_, err := handler.SendRunHumanMessage(context.Background(), connect.NewRequest(&agentcomposev2.SendRunHumanMessageRequest{
		RunId: "done", Text: "next", ClientMessageId: "m1",
	}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("SendRunHumanMessage error code = %s, want %s (error: %v)", connect.CodeOf(err), connect.CodeFailedPrecondition, err)
	}
}

func TestConversationHandlerPreservesDelegateErrorCodes(t *testing.T) {
	ctx := context.Background()
	store := newRunSupervisorTestConfigStore(t)
	if _, err := store.UpsertProject(ctx, domain.ProjectRecord{ID: "project-1", Name: "project", SourcePath: "/tmp/project", SourceJSON: "{}"}); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}
	agent, err := store.UpsertProjectAgent(ctx, domain.ProjectAgentRecord{ProjectID: "project-1", AgentName: "worker"})
	if err != nil {
		t.Fatalf("UpsertProjectAgent: %v", err)
	}
	if _, err := store.CreateProjectRun(ctx, domain.ProjectRunRecord{RunID: "completed-conversation", ProjectID: "project-1", AgentName: "worker", AgentID: agent.ID, Prompt: "continue", InteractiveConversation: true, Status: domain.ProjectRunStatusSucceeded, ResultJSON: "{}"}); err != nil {
		t.Fatalf("CreateProjectRun: %v", err)
	}
	controller := runs.NewController(runs.ControllerDependencies{ConfigDB: store})
	supervisor := &RunSupervisor{root: ctx, controller: controller, store: store, conversations: runs.NewConversationManager(runs.ConversationManagerDeps{Root: ctx, Controller: controller})}
	handler := api.NewRunHandler(runControllerDelegate{supervisor: supervisor}, store)

	for _, test := range []struct {
		name  string
		runID string
		want  connect.Code
	}{
		{name: "not found", runID: "missing-conversation", want: connect.CodeNotFound},
		{name: "not waiting", runID: "completed-conversation", want: connect.CodeFailedPrecondition},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := handler.SendRunHumanMessage(ctx, connect.NewRequest(&agentcomposev2.SendRunHumanMessageRequest{RunId: test.runID, Text: "continue", ClientMessageId: "message-1"}))
			if got := connect.CodeOf(err); got != test.want {
				t.Fatalf("SendRunHumanMessage code = %s, want %s (error: %v)", got, test.want, err)
			}
		})
	}
}

func TestSendRunHumanMessageRejectsEmptyText(t *testing.T) {
	delegate := &conversationRunDelegateFake{}
	handler := api.NewRunHandler(delegate, nil)

	_, err := handler.SendRunHumanMessage(context.Background(), connect.NewRequest(&agentcomposev2.SendRunHumanMessageRequest{
		RunId: "waiting", Text: " \t ", ClientMessageId: "m1",
	}))
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("SendRunHumanMessage error code = %s, want %s (error: %v)", connect.CodeOf(err), connect.CodeInvalidArgument, err)
	}
	if delegate.replyCalls != 0 {
		t.Fatalf("SendRunHumanMessage delegated %d times for empty text", delegate.replyCalls)
	}
}

type conversationRunDelegateFake struct {
	replyErr   error
	replyCalls int
}

func (*conversationRunDelegateFake) RunAgent(context.Context, *connect.Request[agentcomposev2.RunAgentRequest]) (*connect.Response[agentcomposev2.RunAgentResponse], error) {
	return nil, errors.New("not implemented")
}

func (*conversationRunDelegateFake) StartAgentRun(context.Context, *connect.Request[agentcomposev2.StartAgentRunRequest]) (*connect.Response[agentcomposev2.StartAgentRunResponse], error) {
	return nil, errors.New("not implemented")
}

func (*conversationRunDelegateFake) StreamAgentRun(context.Context, *connect.Request[agentcomposev2.RunAgentRequest], *connect.ServerStream[agentcomposev2.StreamAgentRunResponse]) error {
	return errors.New("not implemented")
}

func (*conversationRunDelegateFake) AttachAgentRun(context.Context, *connect.BidiStream[agentcomposev2.AttachAgentRunRequest, agentcomposev2.AttachAgentRunResponse]) error {
	return errors.New("not implemented")
}

func (*conversationRunDelegateFake) StartInteractiveAgentRun(context.Context, *connect.Request[agentcomposev2.StartAgentRunRequest]) (*connect.Response[agentcomposev2.StartAgentRunResponse], error) {
	return nil, errors.New("not implemented")
}

func (d *conversationRunDelegateFake) SendRunHumanMessage(context.Context, *connect.Request[agentcomposev2.SendRunHumanMessageRequest]) (*connect.Response[agentcomposev2.SendRunHumanMessageResponse], error) {
	d.replyCalls++
	if d.replyErr != nil {
		return nil, d.replyErr
	}
	return connect.NewResponse(&agentcomposev2.SendRunHumanMessageResponse{}), nil
}

var _ api.RunAgentDelegate = (*conversationRunDelegateFake)(nil)
var _ = runs.StatusIsTerminal
