package agentcomposev2

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestExecutionRPCNamesAndStreamingShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		service         protoreflect.Name
		method          protoreflect.Name
		input           protoreflect.FullName
		output          protoreflect.FullName
		clientStreaming bool
		serverStreaming bool
	}{
		{"RunService", "RunAgent", "agentcompose.v2.RunAgentRequest", "agentcompose.v2.RunAgentResponse", false, false},
		{"RunService", "StartAgentRun", "agentcompose.v2.StartAgentRunRequest", "agentcompose.v2.StartAgentRunResponse", false, false},
		{"RunService", "StartInteractiveAgentRun", "agentcompose.v2.StartAgentRunRequest", "agentcompose.v2.StartAgentRunResponse", false, false},
		{"RunService", "SendRunHumanMessage", "agentcompose.v2.SendRunHumanMessageRequest", "agentcompose.v2.SendRunHumanMessageResponse", false, false},
		{"RunService", "StreamAgentRun", "agentcompose.v2.RunAgentRequest", "agentcompose.v2.StreamAgentRunResponse", false, true},
		{"RunService", "AttachAgentRun", "agentcompose.v2.AttachAgentRunRequest", "agentcompose.v2.AttachAgentRunResponse", true, true},
		{"ExecService", "Exec", "agentcompose.v2.ExecRequest", "agentcompose.v2.ExecResponse", false, false},
		{"ExecService", "StreamExec", "agentcompose.v2.ExecRequest", "agentcompose.v2.StreamExecResponse", false, true},
		{"ExecService", "AttachExec", "agentcompose.v2.AttachExecRequest", "agentcompose.v2.AttachExecResponse", true, true},
		{"ProjectService", "InvokeScheduler", "agentcompose.v2.InvokeSchedulerRequest", "agentcompose.v2.InvokeSchedulerResponse", false, false},
		{"ProjectService", "RunScheduler", "agentcompose.v2.RunSchedulerRequest", "agentcompose.v2.RunSchedulerResponse", false, false},
		{"ProjectService", "StartSchedulerRun", "agentcompose.v2.StartSchedulerRunRequest", "agentcompose.v2.StartSchedulerRunResponse", false, false},
	}

	services := File_agentcompose_v2_agentcompose_proto.Services()
	for _, test := range tests {
		t.Run(string(test.service)+"/"+string(test.method), func(t *testing.T) {
			service := services.ByName(test.service)
			if service == nil {
				t.Fatalf("service %q not found", test.service)
			}
			method := service.Methods().ByName(test.method)
			if method == nil {
				t.Fatalf("method %q not found", test.method)
			}
			if got := method.Input().FullName(); got != test.input {
				t.Errorf("input = %q, want %q", got, test.input)
			}
			if got := method.Output().FullName(); got != test.output {
				t.Errorf("output = %q, want %q", got, test.output)
			}
			if got := method.IsStreamingClient(); got != test.clientStreaming {
				t.Errorf("client streaming = %t, want %t", got, test.clientStreaming)
			}
			if got := method.IsStreamingServer(); got != test.serverStreaming {
				t.Errorf("server streaming = %t, want %t", got, test.serverStreaming)
			}
		})
	}
}

func TestRunServiceConversationContract(t *testing.T) {
	t.Parallel()

	assertProcedure(t, "RunService", "StartInteractiveAgentRun", false, false)
	assertProcedure(t, "RunService", "SendRunHumanMessage", false, false)
}

func TestRunConversationMessageContract(t *testing.T) {
	t.Parallel()

	assertMessageFields(t, "SendRunHumanMessageRequest", []struct {
		name   protoreflect.Name
		number protoreflect.FieldNumber
		kind   protoreflect.Kind
	}{
		{"run_id", 1, protoreflect.StringKind},
		{"text", 2, protoreflect.StringKind},
		{"client_message_id", 3, protoreflect.StringKind},
	})
	assertMessageFields(t, "SendRunHumanMessageResponse", []struct {
		name   protoreflect.Name
		number protoreflect.FieldNumber
		kind   protoreflect.Kind
	}{
		{"run", 1, protoreflect.MessageKind},
	})
	assertMessageFieldType(t, "SendRunHumanMessageResponse", "run", "agentcompose.v2.RunSummary")
}

func TestRunConversationStatusNumbers(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		got  RunStatus
		want protoreflect.EnumNumber
	}{
		{"waiting for input", RunStatus_RUN_STATUS_WAITING_FOR_INPUT, 6},
		{"timed out", RunStatus_RUN_STATUS_TIMED_OUT, 7},
		{"interrupted", RunStatus_RUN_STATUS_INTERRUPTED, 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.got.Number(); got != test.want {
				t.Fatalf("status number = %d, want %d", got, test.want)
			}
		})
	}
}

func assertProcedure(t *testing.T, serviceName, methodName protoreflect.Name, clientStreaming, serverStreaming bool) {
	t.Helper()

	service := File_agentcompose_v2_agentcompose_proto.Services().ByName(serviceName)
	if service == nil {
		t.Fatalf("service %q not found", serviceName)
	}
	method := service.Methods().ByName(methodName)
	if method == nil {
		t.Fatalf("method %q not found", methodName)
	}
	if got := method.IsStreamingClient(); got != clientStreaming {
		t.Errorf("client streaming = %t, want %t", got, clientStreaming)
	}
	if got := method.IsStreamingServer(); got != serverStreaming {
		t.Errorf("server streaming = %t, want %t", got, serverStreaming)
	}
}

func assertMessageFields(t *testing.T, messageName protoreflect.Name, want []struct {
	name   protoreflect.Name
	number protoreflect.FieldNumber
	kind   protoreflect.Kind
}) {
	t.Helper()

	message := File_agentcompose_v2_agentcompose_proto.Messages().ByName(messageName)
	if message == nil {
		t.Fatalf("message %q not found", messageName)
	}
	for _, field := range want {
		got := message.Fields().ByName(field.name)
		if got == nil {
			t.Errorf("field %q not found", field.name)
			continue
		}
		if got.Number() != field.number {
			t.Errorf("field %q number = %d, want %d", field.name, got.Number(), field.number)
		}
		if got.Kind() != field.kind {
			t.Errorf("field %q kind = %s, want %s", field.name, got.Kind(), field.kind)
		}
	}
}

func assertMessageFieldType(t *testing.T, messageName, fieldName protoreflect.Name, want protoreflect.FullName) {
	t.Helper()

	message := File_agentcompose_v2_agentcompose_proto.Messages().ByName(messageName)
	if message == nil {
		t.Fatalf("message %q not found", messageName)
	}
	field := message.Fields().ByName(fieldName)
	if field == nil {
		t.Fatalf("field %q not found", fieldName)
	}
	if got := field.Message().FullName(); got != want {
		t.Errorf("field %q message type = %q, want %q", fieldName, got, want)
	}
}

func TestLegacyExecutionRPCNamesAreNotExposed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		service protoreflect.Name
		methods []protoreflect.Name
	}{
		{"RunService", []protoreflect.Name{"StartRun", "RunAgentStream", "RunAttach"}},
		{"ExecService", []protoreflect.Name{"ExecStream", "ExecAttach"}},
	}

	services := File_agentcompose_v2_agentcompose_proto.Services()
	for _, test := range tests {
		service := services.ByName(test.service)
		if service == nil {
			t.Fatalf("service %q not found", test.service)
		}
		for _, method := range test.methods {
			if service.Methods().ByName(method) != nil {
				t.Errorf("legacy method %s/%s is still exposed", test.service, method)
			}
		}
	}
}
