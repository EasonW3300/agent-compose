package schedulers

import (
	"context"
	"fmt"
	"strings"

	domain "agent-compose/pkg/model"

	"github.com/google/uuid"
)

type HostProjectAgentRequest struct {
	SchedulerID        string
	SchedulerRunID     string
	ProjectID          string
	AgentName          string
	Prompt             string
	ProjectSchedulerID string
	TriggerID          string
	OutputSchemaJSON   string
	ClientRequestID    string
	Volumes            []domain.VolumeMountSpec
	SandboxPolicy      string
	SandboxConfigHash  string
}

type HostProjectAgentRunner interface {
	RunProjectAgent(ctx context.Context, request HostProjectAgentRequest) (domain.ProjectRunRecord, error, error)
}

// HostProjectRunLister narrows scheduler reads to the project-run query needed
// to prevent a scheduled prompt from competing with an active conversation.
type HostProjectRunLister interface {
	ListProjectRunsByOptions(context.Context, domain.ProjectRunListOptions) ([]domain.ProjectRunRecord, error)
}

func (h *RuntimeHost) ProjectAgent(ctx context.Context, prompt string, request domain.SchedulerAgentRequest) (domain.SchedulerAgentResult, error) {
	sandboxPolicy := domain.SchedulerSandboxPolicyNew
	if strings.TrimSpace(h.scheduler.Summary.SandboxPolicy) != "" {
		sandboxPolicy = NormalizeSandboxPolicy(h.scheduler.Summary.SandboxPolicy)
	}
	if strings.TrimSpace(AgentSandboxPolicy(request)) != "" {
		sandboxPolicy = NormalizeSandboxPolicy(AgentSandboxPolicy(request))
	}
	configHash, err := SchedulerSandboxConfigHash(h.scheduler)
	if err != nil {
		return domain.SchedulerAgentResult{}, err
	}
	// Check before calling ProjectAgentRunner: that call owns workspace and
	// sandbox preparation, which must not run while a user-facing prompt waits.
	if active, err := h.hasActiveConversation(ctx); err != nil {
		return domain.SchedulerAgentResult{}, err
	} else if active {
		_ = h.addSchedulerEvent(ctx, SchedulerEventInput{
			EventType: "scheduler.skipped_active_conversation", Level: "warn",
			Message: "scheduler skipped because the project agent is waiting for input",
			Payload: map[string]any{"reason": "active_conversation"},
		})
		return domain.SchedulerAgentResult{}, ErrActiveConversation
	}
	run, execErr, err := h.deps.ProjectAgentRunner.RunProjectAgent(ctx, HostProjectAgentRequest{
		SchedulerID:        h.scheduler.Summary.ID,
		SchedulerRunID:     h.execution.ID,
		ProjectID:          h.scheduler.Summary.ProjectID,
		AgentName:          h.scheduler.Summary.AgentName,
		Prompt:             prompt,
		ProjectSchedulerID: h.scheduler.Summary.ProjectSchedulerID,
		TriggerID:          h.execution.TriggerID,
		OutputSchemaJSON:   request.OutputSchema,
		ClientRequestID:    h.nextProjectAgentRunID(),
		Volumes:            request.Volumes,
		SandboxPolicy:      sandboxPolicy,
		SandboxConfigHash:  configHash,
	})
	if err != nil {
		return domain.SchedulerAgentResult{}, err
	}
	result, jsonErr := AgentResultFromProjectRun(run, request.OutputSchema)
	if jsonErr != nil && execErr == nil {
		execErr = jsonErr
	}
	level := "info"
	eventName := "scheduler.agent.completed"
	if execErr != nil || run.Status != domain.ProjectRunStatusSucceeded {
		level = "error"
		eventName = "scheduler.agent.failed"
		result.Text = firstHostNonEmpty(result.Text, run.Error, execErrString(execErr))
	}
	_ = h.addLinkedSchedulerEvent(ctx, SchedulerEventInput{
		EventType:           eventName,
		Level:               level,
		Message:             firstHostNonEmpty(result.Text, fmt.Sprintf("%s completed", result.Agent)),
		Payload:             result,
		LinkedSandboxID:     result.SandboxID,
		LinkedCellID:        result.CellID,
		LinkedAgentThreadID: result.AgentThreadID,
	})
	h.publishAgentCompleted(result, &run)
	if execErr != nil {
		return result, execErr
	}
	return result, nil
}

func (h *RuntimeHost) hasActiveConversation(ctx context.Context) (bool, error) {
	if h.deps.ProjectRuns == nil {
		return false, nil
	}
	const projectRunPageSize = 200
	for _, status := range []string{domain.ProjectRunStatusRunning, domain.ProjectRunStatusWaitingForInput} {
		for offset := 0; ; {
			items, err := h.deps.ProjectRuns.ListProjectRunsByOptions(ctx, domain.ProjectRunListOptions{
				ProjectID: h.scheduler.Summary.ProjectID, AgentName: h.scheduler.Summary.AgentName,
				Status: status, Limit: projectRunPageSize, Offset: offset,
			})
			if err != nil {
				return false, fmt.Errorf("list active project conversations: %w", err)
			}
			for _, run := range items {
				// Only the durable interactive marker denotes a reply-capable stream.
				// A normal single-turn prompt may be running concurrently with a schedule.
				if run.InteractiveConversation {
					return true, nil
				}
			}
			if len(items) < projectRunPageSize {
				break
			}
			offset += len(items)
		}
	}
	return false, nil
}

func (h *RuntimeHost) nextProjectAgentRunID() string {
	baseID := firstHostNonEmpty(h.execution.ID, uuid.NewString())
	return fmt.Sprintf("%s:agent:%d", baseID, h.projectAgentRunSequence.Add(1))
}

func (h *RuntimeHost) useProjectAgentRun(request domain.SchedulerAgentRequest) bool {
	if strings.TrimSpace(h.scheduler.Summary.ProjectID) == "" || strings.TrimSpace(h.scheduler.Summary.AgentName) == "" {
		return false
	}
	if strings.TrimSpace(request.Agent) != "" || request.Timeout > 0 {
		return false
	}
	return !AgentRequestOverridesSession(request, true)
}
