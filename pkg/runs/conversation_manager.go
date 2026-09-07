package runs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	driverpkg "agent-compose/pkg/driver"
	domain "agent-compose/pkg/model"
	"agent-compose/pkg/projects"
)

// driverpkg classifies runtime termination errors that must retain their
// managed LLM token; projects supplies the stable ID policy for run changes.

const defaultConversationTimeout = 24 * time.Hour

// ConversationManagerDeps supplies the daemon-owned dependencies for prompt
// conversations. Timeout is injectable so lifecycle tests need not wait a day.
type ConversationManagerDeps struct {
	Root       context.Context
	Controller *Controller
	Timeout    time.Duration
}

// conversationRunLister is intentionally limited to startup recovery. Normal
// interaction work uses Controller's narrower Store contract.
type conversationRunLister interface {
	ListProjectRunsByOptions(context.Context, domain.ProjectRunListOptions) ([]domain.ProjectRunRecord, error)
}

// conversationMessageLookup reads durable user events so reconnect retries can
// be acknowledged after the first reply has already advanced the run state.
type conversationMessageLookup interface {
	ListProjectRunEvents(context.Context, string, uint64, int) ([]domain.ProjectRunEventRecord, error)
}

// ConversationManager holds live prompt interactions independently of an HTTP
// stream. It is the only owner allowed to close an interaction that has entered
// the detached conversation lifecycle.
type ConversationManager struct {
	root       context.Context
	controller *Controller
	store      Store
	lister     conversationRunLister
	timeout    time.Duration

	mu            sync.Mutex
	conversations map[string]*managedConversation
	shuttingDown  bool
	wg            sync.WaitGroup
}

// managedConversation bundles the runtime session, cancellation path, and
// reply serialization for one persisted project run.
type managedConversation struct {
	runID       string
	context     context.Context
	cancel      context.CancelCauseFunc
	coordinator *Coordinator
	session     promptInteractionSession

	replyMu    sync.Mutex
	finishOnce sync.Once
	done       chan struct{}
}

// NewConversationManager creates a manager without starting work. Call
// RecoverInterrupted during daemon startup before exposing reply handlers.
func NewConversationManager(deps ConversationManagerDeps) *ConversationManager {
	root := deps.Root
	if root == nil {
		root = context.Background()
	}
	timeout := deps.Timeout
	if timeout <= 0 {
		timeout = defaultConversationTimeout
	}
	manager := &ConversationManager{
		root: root, controller: deps.Controller, timeout: timeout, conversations: map[string]*managedConversation{},
	}
	if deps.Controller != nil {
		manager.store = deps.Controller.configDB
		manager.lister, _ = deps.Controller.configDB.(conversationRunLister)
	}
	return manager
}

// Start opens a daemon-owned prompt interaction. The request context creates
// the durable run, while all subsequent runtime work derives from daemon root
// context so a browser disconnect cannot end the conversation.
func (m *ConversationManager) Start(ctx context.Context, req RunAgentRequest) (domain.ProjectRunRecord, error) {
	if m == nil || m.controller == nil || m.store == nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("conversation manager is not configured")
	}
	if strings.TrimSpace(req.Prompt) == "" || strings.TrimSpace(req.Command) != "" {
		return domain.ProjectRunRecord{}, fmt.Errorf("%w: conversations require a prompt and do not support commands", ErrInvalidRequest)
	}
	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return domain.ProjectRunRecord{}, context.Canceled
	}
	// Count this start before releasing the lock so Shutdown cannot return while
	// setup is about to register a runtime-owning goroutine.
	m.wg.Add(1)
	m.mu.Unlock()
	startedWorker := false
	defer func() {
		if !startedWorker {
			m.wg.Done()
		}
	}()

	started, err := m.controller.StartProjectRun(ctx, req)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	if NormalizeStatus(started.Run.Status) != domain.ProjectRunStatusPending {
		return started.Run, nil
	}
	// Preserve only trusted request metadata while deliberately detaching from
	// transport cancellation for workspace, sandbox, and runtime operations.
	execCtx := domain.NewContextWithTrustedHeaders(context.WithoutCancel(m.root), domain.TrustedHeadersFromContext(ctx))
	runCtx, cancel := context.WithCancelCause(execCtx)
	session, run, err := m.openPromptConversation(runCtx, started.Run, req)
	if err != nil {
		cancel(err)
		return run, err
	}
	conversation := &managedConversation{
		runID: run.RunID, context: runCtx, cancel: cancel, coordinator: NewCoordinator(m.store, projects.StableProjectRunID), session: session, done: make(chan struct{}),
	}
	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		conversation.replyMu.Lock()
		_ = conversation.session.Input.Cancel("daemon shutting down")
		conversation.replyMu.Unlock()
		cancel(context.Canceled)
		return run, context.Canceled
	}
	m.conversations[run.RunID] = conversation
	m.mu.Unlock()
	startedWorker = true
	go m.runConversation(conversation)
	return run, nil
}

// Reply accepts one reply only while the persisted run is waiting. The user
// event's client ID is committed before writing to runtime stdin, ensuring
// reconnect retries do not create a second human frame.
func (m *ConversationManager) Reply(_ context.Context, runID, text, clientMessageID string) (domain.ProjectRunRecord, error) {
	if m == nil || m.store == nil {
		return domain.ProjectRunRecord{}, fmt.Errorf("conversation manager is not configured")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return domain.ProjectRunRecord{}, fmt.Errorf("run id is required")
	}
	m.mu.Lock()
	conversation := m.conversations[runID]
	m.mu.Unlock()
	if conversation == nil {
		return m.loadConversationReplyRun(runID, "conversation is not active")
	}
	conversation.replyMu.Lock()
	defer conversation.replyMu.Unlock()
	run, err := m.store.GetProjectRun(context.WithoutCancel(conversation.context), runID)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	if run.Status != domain.ProjectRunStatusWaitingForInput {
		// The client message is committed before the state transition to RUNNING.
		// Check its durable record first so retrying a timed-out HTTP response is
		// idempotent while genuinely new messages still fail precondition.
		if duplicate, lookupErr := m.hasHumanMessageClientID(conversation.context, runID, clientMessageID); lookupErr != nil {
			return domain.ProjectRunRecord{}, lookupErr
		} else if duplicate {
			return run, nil
		}
		return run, domain.ResourceError(domain.ErrFailedPrecondition, "conversation", runID, fmt.Sprintf("conversation %s cannot accept a reply while %s", runID, run.Status), nil)
	}
	created, err := conversation.session.Projector.AppendHumanMessageFrameIfNew(text, clientMessageID)
	if err != nil {
		return run, err
	}
	if !created {
		return run, nil
	}
	// The waiting transition is durable before its gate opens. Record the resume
	// before handing bytes to the runtime so observers never see an unpersisted
	// human turn in progress.
	run, err = conversation.coordinator.MarkRunning(context.WithoutCancel(conversation.context), runID, conversation.session.Sandbox.Summary.ID)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	select {
	case <-conversation.context.Done():
		return run, context.Cause(conversation.context)
	case <-conversation.session.TurnReady:
	}
	if err := conversation.session.Input.HumanMessage(text); err != nil {
		m.terminateConversation(conversation, domain.ProjectRunStatusFailed, fmt.Sprintf("send conversation reply: %v", err))
		return run, err
	}
	return run, nil
}

func (m *ConversationManager) hasHumanMessageClientID(ctx context.Context, runID, clientMessageID string) (bool, error) {
	clientMessageID = strings.TrimSpace(clientMessageID)
	if clientMessageID == "" {
		return false, nil
	}
	lookup, ok := m.store.(conversationMessageLookup)
	if !ok {
		return false, nil
	}
	for after := uint64(0); ; {
		events, err := lookup.ListProjectRunEvents(context.WithoutCancel(ctx), runID, after, 200)
		if err != nil {
			return false, fmt.Errorf("list conversation messages: %w", err)
		}
		if len(events) == 0 {
			return false, nil
		}
		for _, event := range events {
			if event.Sequence > after {
				after = event.Sequence
			}
			if event.Kind != domain.ProjectRunEventKindUserMessage || strings.TrimSpace(event.PayloadJSON) == "" {
				continue
			}
			var payload struct {
				ClientMessageID string `json:"client_message_id"`
			}
			if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
				continue
			}
			if strings.TrimSpace(payload.ClientMessageID) == clientMessageID {
				return true, nil
			}
		}
		if len(events) < 200 {
			return false, nil
		}
	}
}

// Stop cancels an active conversation and records a canceled terminal state.
// The asynchronous signature mirrors supervisor cancellation and intentionally
// leaves persistence errors to the runtime lifecycle owner.
func (m *ConversationManager) Stop(runID, reason string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	conversation := m.conversations[strings.TrimSpace(runID)]
	m.mu.Unlock()
	if conversation == nil {
		return
	}
	m.terminateConversation(conversation, domain.ProjectRunStatusCanceled, firstNonEmpty(reason, "conversation stopped"))
}

// RecoverInterrupted turns orphaned live conversations from a prior daemon
// process into terminal interrupted runs before any new manager can accept a
// reply for them.
func (m *ConversationManager) RecoverInterrupted(ctx context.Context) error {
	if m == nil || m.controller == nil || m.store == nil || m.lister == nil {
		return nil
	}
	for _, status := range []string{domain.ProjectRunStatusRunning, domain.ProjectRunStatusWaitingForInput} {
		var candidates []domain.ProjectRunRecord
		// Collect every status-filtered page before changing any run. Each
		// interruption removes that run from this query, so writing during offset
		// pagination would shift later records and leave conversations unrecovered.
		for offset := 0; ; {
			items, err := m.lister.ListProjectRunsByOptions(ctx, domain.ProjectRunListOptions{Status: status, Limit: 200, Offset: offset})
			if err != nil {
				return fmt.Errorf("list %s conversations for recovery: %w", status, err)
			}
			if len(items) == 0 {
				break
			}
			candidates = append(candidates, items...)
			offset += len(items)
		}
		for _, run := range candidates {
			// A persisted prompt identifies the runtime stream as a human-turn
			// conversation. Running command runs are recovered by their existing
			// supervisor path and must not be relabeled here.
			if StatusIsTerminal(run.Status) || strings.TrimSpace(run.Prompt) == "" {
				continue
			}
			if _, err := m.controller.completeProjectRun(context.WithoutCancel(ctx), TransitionRequest{
				RunID: run.RunID, Status: domain.ProjectRunStatusInterrupted, SandboxID: run.SandboxID, Error: "conversation interrupted by daemon restart",
			}); err != nil {
				return fmt.Errorf("mark conversation %s interrupted: %w", run.RunID, err)
			}
		}
	}
	return nil
}

// Shutdown releases local runtime ownership without turning conversations into
// canceled runs. A subsequent daemon startup recovers their durable running or
// waiting records as interrupted.
func (m *ConversationManager) Shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	m.shuttingDown = true
	items := make([]*managedConversation, 0, len(m.conversations))
	for _, conversation := range m.conversations {
		items = append(items, conversation)
	}
	m.mu.Unlock()
	for _, conversation := range items {
		conversation.replyMu.Lock()
		_ = conversation.session.Input.Cancel("daemon shutting down")
		conversation.replyMu.Unlock()
		conversation.cancel(context.Canceled)
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *ConversationManager) openPromptConversation(ctx context.Context, run domain.ProjectRunRecord, req RunAgentRequest) (promptInteractionSession, domain.ProjectRunRecord, error) {
	transitionCtx := context.WithoutCancel(ctx)
	coordinator := NewCoordinator(m.store, projects.StableProjectRunID)
	prepared, err := m.controller.prepareProjectRun(ctx, run, req.Env)
	if err != nil {
		return m.failConversationStart(transitionCtx, ctx, run, TransitionRequest{RunID: run.RunID, Error: fmt.Sprintf("workspace preparation failed: %v", err)}, err)
	}
	sandboxResult, err := m.controller.ensureProjectRunSandbox(ctx, run, prepared, req)
	if err != nil {
		transition := TransitionRequest{RunID: run.RunID, Error: fmt.Sprintf("sandbox start failed: %v", err)}
		if sandboxResult.Sandbox != nil {
			transition.SandboxID = sandboxResult.Sandbox.Summary.ID
		}
		return m.failConversationStart(transitionCtx, ctx, run, transition, err)
	}
	run, err = coordinator.MarkRunning(transitionCtx, run.RunID, sandboxResult.Sandbox.Summary.ID)
	if err != nil {
		return promptInteractionSession{}, domain.ProjectRunRecord{}, err
	}
	runCtx := interactionRunContext{Coordinator: coordinator, Run: run, Sandbox: sandboxResult.Sandbox, Request: req}
	preparedPrompt, err := m.controller.preparePromptInteractionRuntime(ctx, runCtx)
	if err != nil {
		transition := TransitionRequest{RunID: run.RunID, SandboxID: sandboxResult.Sandbox.Summary.ID, LogsPath: projectRunCommandArtifactsDir(run, sandboxResult.Sandbox), ExitCode: 1, Error: fmt.Sprintf("agent execution failed: %v", err)}
		return m.failConversationStart(transitionCtx, ctx, run, transition, err)
	}
	session, err := m.controller.openPromptInteractionSession(ctx, runCtx, preparedPrompt)
	if err != nil {
		transition := TransitionRequest{RunID: run.RunID, SandboxID: sandboxResult.Sandbox.Summary.ID, LogsPath: preparedPrompt.LogsPath, ExitCode: 1, Error: fmt.Sprintf("agent execution failed: %v", err)}
		return m.failConversationStart(transitionCtx, ctx, run, transition, err)
	}
	return session, preparedPrompt.Run, nil
}

func (m *ConversationManager) failConversationStart(transitionCtx, executionCtx context.Context, run domain.ProjectRunRecord, transition TransitionRequest, executionErr error) (promptInteractionSession, domain.ProjectRunRecord, error) {
	updated, markErr := m.controller.completeProjectRunError(transitionCtx, executionCtx, transition, executionErr)
	if markErr != nil {
		return promptInteractionSession{}, domain.ProjectRunRecord{}, markErr
	}
	return promptInteractionSession{}, updated, executionErr
}

func (m *ConversationManager) runConversation(conversation *managedConversation) {
	defer m.wg.Done()
	defer close(conversation.done)
	defer m.unregisterConversation(conversation)
	defer func() { _ = conversation.session.Interaction.CloseSend() }()
	defer m.deleteConversationToken(conversation)
	timer := time.NewTimer(m.timeout)
	defer timer.Stop()
	go func() {
		select {
		case <-conversation.done:
		case <-timer.C:
			m.terminateConversation(conversation, domain.ProjectRunStatusTimedOut, "conversation timed out")
		}
	}()

	transition, err := receivePromptInteractionFrames(conversation.context, conversation.session.Run, conversation.session.Sandbox, conversation.session.Transition, promptInteractionReceiveState{
		Interaction: conversation.session.Interaction, Projector: conversation.session.Projector, Coordinator: conversation.coordinator, TurnReady: conversation.session.TurnReady,
	}, func(RunAttachOutput) error { return nil })
	if m.isShuttingDown() {
		return
	}
	if err != nil || transition.ExitCode != 0 {
		m.terminateConversation(conversation, domain.ProjectRunStatusFailed, firstNonEmpty(transition.Error, errorText(err), "conversation runtime failed"))
		return
	}
	if transition.Status == domain.ProjectRunStatusWaitingForInput {
		m.terminateConversation(conversation, domain.ProjectRunStatusFailed, "conversation runtime exited while waiting for input")
		return
	}
	m.terminateConversation(conversation, domain.ProjectRunStatusSucceeded, "")
}

func (m *ConversationManager) terminateConversation(conversation *managedConversation, status, reason string) {
	conversation.finishOnce.Do(func() {
		transitionCtx := context.WithoutCancel(conversation.context)
		current, err := m.store.GetProjectRun(transitionCtx, conversation.runID)
		if err == nil && !StatusIsTerminal(current.Status) {
			_, _ = m.controller.completeProjectRun(transitionCtx, TransitionRequest{
				RunID: conversation.runID, Status: status, SandboxID: conversation.session.Sandbox.Summary.ID, Error: reason,
			})
		}
		conversation.replyMu.Lock()
		_ = conversation.session.Input.Cancel(reason)
		conversation.replyMu.Unlock()
		conversation.cancel(errors.New(firstNonEmpty(reason, string(status))))
	})
}

func (m *ConversationManager) loadConversationReplyRun(runID, reason string) (domain.ProjectRunRecord, error) {
	run, err := m.store.GetProjectRun(context.WithoutCancel(m.root), runID)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	return run, domain.ResourceError(domain.ErrFailedPrecondition, "conversation", runID, fmt.Sprintf("conversation %s cannot accept a reply: %s", runID, reason), nil)
}

func (m *ConversationManager) unregisterConversation(conversation *managedConversation) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.conversations[conversation.runID] == conversation {
		delete(m.conversations, conversation.runID)
	}
}

func (m *ConversationManager) isShuttingDown() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.shuttingDown
}

func (m *ConversationManager) deleteConversationToken(conversation *managedConversation) {
	token := conversation.session.ManagedToken
	if token == "" {
		return
	}
	if err := context.Cause(conversation.context); errors.Is(err, domain.ErrExecTerminationUnconfirmed) || errors.Is(err, driverpkg.ErrExecTerminationUnconfirmed) {
		return
	}
	m.controller.deletePromptAttachLLMFacadeToken(context.WithoutCancel(conversation.context), token)
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
