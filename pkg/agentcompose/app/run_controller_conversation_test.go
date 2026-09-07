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
