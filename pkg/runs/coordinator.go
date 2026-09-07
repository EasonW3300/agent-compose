package runs

import (
	driverpkg "agent-compose/pkg/driver"
	domain "agent-compose/pkg/model"
	"agent-compose/pkg/projects"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// domain provides durable run/event records, projects resolves immutable agent
// definitions, and driverpkg validates the configured sandbox runtime.

type StartRequest struct {
	ProjectID               string
	AgentName               string
	Source                  string
	SchedulerID             string
	SchedulerRunID          string
	TriggerID               string
	Prompt                  string
	Driver                  string
	CleanupPolicy           string
	ClientRequestID         string
	InteractiveConversation bool
}

type TransitionRequest struct {
	RunID        string
	Status       string
	SandboxID    string
	ExitCode     int
	Error        string
	ErrorStack   string
	Output       string
	ResultJSON   string
	LogsPath     string
	ArtifactsDir string
	CleanupError string
	// StatusEventID optionally pins the durable ID for an individual lifecycle
	// edge. Interactive resumes use a fresh ID because the same edge may recur.
	StatusEventID string
	// TerminalEvents are conversation records committed with this state update.
	// Waiting transitions use the same batch so an agent message cannot outlive
	// the status that explains why the turn paused.
	TerminalEvents []domain.ProjectRunEventRecord
}

type Store interface {
	GetProject(context.Context, string) (domain.ProjectRecord, error)
	GetProjectRevision(context.Context, string, int64) (domain.ProjectRevisionRecord, error)
	GetProjectAgent(context.Context, string, string) (domain.ProjectAgentRecord, error)
	CreateProjectRun(context.Context, domain.ProjectRunRecord) (domain.ProjectRunRecord, error)
	CreateProjectRunWithEvents(context.Context, domain.ProjectRunRecord, []domain.ProjectRunEventRecord) (domain.ProjectRunRecord, error)
	GetProjectRun(context.Context, string) (domain.ProjectRunRecord, error)
	UpdateProjectRun(context.Context, domain.ProjectRunRecord) (domain.ProjectRunRecord, error)
	UpdateProjectRunWithEvents(context.Context, domain.ProjectRunRecord, []domain.ProjectRunEventRecord) (domain.ProjectRunRecord, error)
}

type structuredEventStore interface {
	AppendProjectRunEvent(context.Context, domain.ProjectRunEventRecord) (domain.ProjectRunEventRecord, bool, error)
	AppendProjectRunEvents(context.Context, []domain.ProjectRunEventRecord) ([]domain.ProjectRunEventRecord, []bool, error)
}

type StableRunIDFunc func(projectID, agentName, source, idempotencyKey string) (string, error)

type Coordinator struct {
	store       Store
	stableRunID StableRunIDFunc
	now         func() time.Time
}

func NewCoordinator(store Store, stableRunID StableRunIDFunc) *Coordinator {
	return &Coordinator{
		store:       store,
		stableRunID: stableRunID,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (c *Coordinator) SetNow(now func() time.Time) {
	if c == nil {
		return
	}
	c.now = now
}

func (c *Coordinator) BeginRun(ctx context.Context, req StartRequest) (domain.ProjectRunRecord, error) {
	if c == nil || c.store == nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("config store is required")
	}
	if c.stableRunID == nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("stable run id function is required")
	}
	req.ProjectID = strings.TrimSpace(req.ProjectID)
	req.AgentName = strings.TrimSpace(req.AgentName)
	req.Source = NormalizeSource(req.Source)
	req.SchedulerID = strings.TrimSpace(req.SchedulerID)
	req.SchedulerRunID = strings.TrimSpace(req.SchedulerRunID)
	req.TriggerID = strings.TrimSpace(req.TriggerID)
	req.Driver = strings.TrimSpace(req.Driver)
	req.ClientRequestID = strings.TrimSpace(req.ClientRequestID)
	if req.ProjectID == "" || req.AgentName == "" {
		return domain.ProjectRunRecord{}, fmt.Errorf("project id and agent name are required")
	}
	if req.ClientRequestID == "" {
		req.ClientRequestID = uuid.NewString()
	}
	project, err := c.store.GetProject(ctx, req.ProjectID)
	if err != nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("resolve project %s: %w", req.ProjectID, err)
	}
	projectAgent, err := c.store.GetProjectAgent(ctx, project.ID, req.AgentName)
	if err != nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("resolve project agent %s/%s: %w", project.ID, req.AgentName, err)
	}
	revision, err := c.store.GetProjectRevision(ctx, project.ID, project.CurrentRevision)
	if err != nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("resolve project revision %s/%d: %w", project.ID, project.CurrentRevision, err)
	}
	agent, err := projects.AgentDefinitionFromRevision(project, revision, projectAgent.AgentName)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	if !agent.Enabled {
		return domain.ProjectRunRecord{}, fmt.Errorf("project agent %s is disabled", agent.ID)
	}
	driver := firstNonEmpty(req.Driver, agent.Driver, projectAgent.Driver)
	if driver != "" {
		driver, err = driverpkg.ResolveSandboxRuntimeDriver(driver, "")
		if err != nil {
			return domain.ProjectRunRecord{}, err
		}
	}
	runID, err := c.stableRunID(project.ID, projectAgent.AgentName, req.Source, req.ClientRequestID)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	run := domain.ProjectRunRecord{
		RunID:                   runID,
		ProjectID:               project.ID,
		ProjectName:             project.Name,
		ProjectRevision:         project.CurrentRevision,
		AgentName:               projectAgent.AgentName,
		AgentID:                 agent.ID,
		Source:                  req.Source,
		SchedulerID:             req.SchedulerID,
		SchedulerRunID:          req.SchedulerRunID,
		TriggerID:               req.TriggerID,
		Status:                  domain.ProjectRunStatusPending,
		Prompt:                  req.Prompt,
		Driver:                  driver,
		ImageRef:                firstNonEmpty(agent.GuestImage, projectAgent.Image),
		CleanupPolicy:           NormalizeCleanupPolicy(req.CleanupPolicy),
		InteractiveConversation: req.InteractiveConversation,
		ResultJSON:              "{}",
	}
	var initialEvents []domain.ProjectRunEventRecord
	if strings.TrimSpace(run.Prompt) != "" {
		initialEvents = append(initialEvents, domain.ProjectRunEventRecord{ID: initialPromptEventID(run.RunID), RunID: run.RunID, Kind: domain.ProjectRunEventKindUserMessage, Text: run.Prompt, Agent: run.AgentName})
	}
	created, err := c.store.CreateProjectRunWithEvents(ctx, run, initialEvents)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	return created, nil
}

func (c *Coordinator) BindSandbox(ctx context.Context, runID, sandboxID string, created bool) (domain.ProjectRunRecord, error) {
	current, err := c.store.GetProjectRun(ctx, strings.TrimSpace(runID))
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	if StatusIsTerminal(current.Status) {
		return domain.ProjectRunRecord{}, fmt.Errorf("project run %s is already terminal", current.RunID)
	}
	current.SandboxID = strings.TrimSpace(sandboxID)
	current.SandboxCreated = created
	return c.store.UpdateProjectRun(ctx, current)
}

func (c *Coordinator) MarkRunning(ctx context.Context, runID, sessionID string) (domain.ProjectRunRecord, error) {
	return c.TransitionRun(ctx, TransitionRequest{
		RunID:     runID,
		Status:    domain.ProjectRunStatusRunning,
		SandboxID: sessionID,
	})
}

func (c *Coordinator) MarkSucceeded(ctx context.Context, req TransitionRequest) (domain.ProjectRunRecord, error) {
	req.Status = domain.ProjectRunStatusSucceeded
	return c.TransitionRun(ctx, req)
}

func (c *Coordinator) MarkFailed(ctx context.Context, req TransitionRequest) (domain.ProjectRunRecord, error) {
	req.Status = domain.ProjectRunStatusFailed
	return c.TransitionRun(ctx, req)
}

func (c *Coordinator) MarkCanceled(ctx context.Context, req TransitionRequest) (domain.ProjectRunRecord, error) {
	req.Status = domain.ProjectRunStatusCanceled
	return c.TransitionRun(ctx, req)
}

func (c *Coordinator) TransitionRun(ctx context.Context, req TransitionRequest) (domain.ProjectRunRecord, error) {
	if c == nil || c.store == nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("config store is required")
	}
	req.RunID = strings.TrimSpace(req.RunID)
	req.Status = NormalizeStatus(req.Status)
	if req.RunID == "" {
		return domain.ProjectRunRecord{}, fmt.Errorf("run id is required")
	}
	current, err := c.store.GetProjectRun(ctx, req.RunID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ProjectRunRecord{}, err
		}
		return domain.ProjectRunRecord{}, err
	}
	if err := validateProjectRunTransition(current.Status, req.Status); err != nil {
		return domain.ProjectRunRecord{}, err
	}
	now := c.nowUTC()
	next := current
	next.Status = req.Status
	applyProjectRunTransitionFields(&next, req)
	switch req.Status {
	case domain.ProjectRunStatusRunning:
		if next.StartedAt.IsZero() {
			next.StartedAt = now
		}
	case domain.ProjectRunStatusSucceeded, domain.ProjectRunStatusFailed, domain.ProjectRunStatusCanceled, domain.ProjectRunStatusTimedOut, domain.ProjectRunStatusInterrupted:
		// A timeout or interruption can finish a queued run before work ever
		// begins. Preserve that distinction while retaining real running times.
		startedFromPending := current.Status == domain.ProjectRunStatusPending && (req.Status == domain.ProjectRunStatusTimedOut || req.Status == domain.ProjectRunStatusInterrupted)
		if next.StartedAt.IsZero() && !startedFromPending {
			next.StartedAt = now
		}
		if next.CompletedAt.IsZero() {
			next.CompletedAt = now
		}
		if next.StartedAt.IsZero() {
			next.DurationMs = 0
		} else {
			next.DurationMs = max(0, next.CompletedAt.Sub(next.StartedAt).Milliseconds())
		}
	}
	// Message records must survive even when a provider repeats the same
	// nonterminal state. A status record is appended only for an actual state
	// change, so each lifecycle edge has exactly one durable status event.
	batch := make([]domain.ProjectRunEventRecord, 0, len(req.TerminalEvents)+1)
	batch = append(batch, req.TerminalEvents...)
	if projectRunTransitionNeedsStatusEvent(current.Status, next.Status) {
		statusEventID := strings.TrimSpace(req.StatusEventID)
		// A waiting -> running edge has no companion event in this transaction,
		// but may occur once per human reply. Give each occurrence its own ID so
		// the event store does not idempotently collapse later resume cycles.
		if statusEventID == "" && current.Status == domain.ProjectRunStatusWaitingForInput && next.Status == domain.ProjectRunStatusRunning {
			statusEventID = uuid.NewString()
		}
		batch = append(batch, projectRunTransitionStatusEvent(current, next, req.TerminalEvents, statusEventID))
	}
	updated, err := c.store.UpdateProjectRunWithEvents(ctx, next, batch)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	return updated, nil
}

func projectRunTransitionNeedsStatusEvent(from, to string) bool {
	from = NormalizeStatus(from)
	to = NormalizeStatus(to)
	if from == to {
		return false
	}
	return true
}

func projectRunTransitionStatusEvent(previous, next domain.ProjectRunRecord, events []domain.ProjectRunEventRecord, explicitID string) domain.ProjectRunEventRecord {
	// Terminal status IDs stay stable for completion-journal retries. A
	// nonterminal edge includes the accompanying event IDs. A repeated resume
	// may supply a separate ID because it is a new occurrence of the same edge.
	eventID := terminalStatusEventID(next.RunID)
	if !StatusIsTerminal(next.Status) {
		eventID = transitionStatusEventID(next.RunID, previous.Status, next.Status, events)
	}
	if explicitID = strings.TrimSpace(explicitID); explicitID != "" {
		eventID = explicitID
	}
	return domain.ProjectRunEventRecord{
		ID: eventID, RunID: next.RunID, Kind: domain.ProjectRunEventKindStatus,
		PayloadJSON: next.ResultJSON, Success: next.Status == domain.ProjectRunStatusSucceeded,
		ExitCode: next.ExitCode, StopReason: next.Error,
	}
}

func (c *Coordinator) nowUTC() time.Time {
	if c != nil && c.now != nil {
		return c.now().UTC()
	}
	return time.Now().UTC()
}

func applyProjectRunTransitionFields(run *domain.ProjectRunRecord, req TransitionRequest) {
	if sandboxID := strings.TrimSpace(req.SandboxID); sandboxID != "" {
		run.SandboxID = sandboxID
	}
	if req.ExitCode != 0 {
		run.ExitCode = req.ExitCode
	}
	if value := strings.TrimSpace(req.Error); value != "" {
		run.Error = value
	}
	if value := strings.TrimSpace(req.ErrorStack); value != "" {
		run.ErrorStack = value
	}
	if req.Output != "" {
		run.Output = req.Output
	}
	if value := strings.TrimSpace(req.ResultJSON); value != "" {
		run.ResultJSON = value
	}
	if value := strings.TrimSpace(req.LogsPath); value != "" {
		run.LogsPath = value
	}
	if value := strings.TrimSpace(req.ArtifactsDir); value != "" {
		run.ArtifactsDir = value
	}
	if value := strings.TrimSpace(req.CleanupError); value != "" {
		run.CleanupError = value
	}
}

func validateProjectRunTransition(from, to string) error {
	from = NormalizeStatus(from)
	to = NormalizeStatus(to)
	if from == to {
		return nil
	}
	if StatusIsTerminal(from) {
		return fmt.Errorf("project run transition %s -> %s is not allowed: run is already terminal", from, to)
	}
	switch from {
	case domain.ProjectRunStatusPending:
		switch to {
		case domain.ProjectRunStatusRunning, domain.ProjectRunStatusFailed, domain.ProjectRunStatusCanceled, domain.ProjectRunStatusTimedOut, domain.ProjectRunStatusInterrupted:
			return nil
		}
	case domain.ProjectRunStatusRunning:
		switch to {
		case domain.ProjectRunStatusSucceeded, domain.ProjectRunStatusFailed, domain.ProjectRunStatusCanceled, domain.ProjectRunStatusWaitingForInput, domain.ProjectRunStatusTimedOut, domain.ProjectRunStatusInterrupted:
			return nil
		}
	case domain.ProjectRunStatusWaitingForInput:
		switch to {
		case domain.ProjectRunStatusRunning, domain.ProjectRunStatusSucceeded, domain.ProjectRunStatusFailed, domain.ProjectRunStatusCanceled, domain.ProjectRunStatusTimedOut, domain.ProjectRunStatusInterrupted:
			return nil
		}
	}
	return fmt.Errorf("project run transition %s -> %s is not allowed", from, to)
}

func StatusIsTerminal(status string) bool {
	switch NormalizeStatus(status) {
	case domain.ProjectRunStatusSucceeded, domain.ProjectRunStatusFailed, domain.ProjectRunStatusCanceled, domain.ProjectRunStatusTimedOut, domain.ProjectRunStatusInterrupted:
		return true
	default:
		return false
	}
}

func NormalizeSource(source string) string {
	switch strings.ToLower(strings.TrimSpace(source)) {
	case domain.ProjectRunSourceScheduler:
		return domain.ProjectRunSourceScheduler
	case domain.ProjectRunSourceAPI:
		return domain.ProjectRunSourceAPI
	case domain.ProjectRunSourceManual:
		return domain.ProjectRunSourceManual
	default:
		return domain.ProjectRunSourceManual
	}
}

func NormalizeStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case domain.ProjectRunStatusRunning:
		return domain.ProjectRunStatusRunning
	case domain.ProjectRunStatusSucceeded:
		return domain.ProjectRunStatusSucceeded
	case domain.ProjectRunStatusFailed:
		return domain.ProjectRunStatusFailed
	case domain.ProjectRunStatusCanceled:
		return domain.ProjectRunStatusCanceled
	case domain.ProjectRunStatusWaitingForInput:
		return domain.ProjectRunStatusWaitingForInput
	case domain.ProjectRunStatusTimedOut:
		return domain.ProjectRunStatusTimedOut
	case domain.ProjectRunStatusInterrupted:
		return domain.ProjectRunStatusInterrupted
	default:
		return domain.ProjectRunStatusPending
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
