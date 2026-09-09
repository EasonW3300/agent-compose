package runs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	appconfig "agent-compose/pkg/config"
	driverpkg "agent-compose/pkg/driver"
	"agent-compose/pkg/execution"
	"agent-compose/pkg/llms"
	domain "agent-compose/pkg/model"
)

// driverpkg supplies the long-lived stdin/stdout interaction that carries the
// framed runtime protocol; execution builds the command which hosts it.

// preparedPromptInteraction is the resolved agent/runtime state
// preparePromptInteractionRuntime hands back to runPromptInteraction once
// dependencies are validated and the run's prompt/schema artifacts are
// written, ready to open the interactive runtime session.
type preparedPromptInteraction struct {
	Run                domain.ProjectRunRecord
	LogsPath           string
	AgentConfig        execution.AgentConfig
	SchemaPath         string
	Env                map[string]string
	ManagedEnv         map[string]string
	VMState            domain.VMState
	InteractionRuntime InteractionRuntime
}

func (c *Controller) preparePromptInteractionRuntime(ctx context.Context, runCtx interactionRunContext) (preparedPromptInteraction, error) {
	run := runCtx.Run
	sandbox := runCtx.Sandbox
	req := runCtx.Request
	artifactsDir := projectRunCommandArtifactsDir(run, sandbox)
	logsPath := filepath.Join(artifactsDir, "transcript.txt")
	if c.store == nil || c.runtime == nil {
		return preparedPromptInteraction{}, fmt.Errorf("prompt runtime dependencies are required")
	}
	appconfig.ApplyDefaultGuestPaths(c.config)
	vmState, err := c.store.GetVMState(sandbox.Summary.ID)
	if err != nil {
		return preparedPromptInteraction{}, err
	}
	runtime, err := c.runtime(sandbox)
	if err != nil {
		return preparedPromptInteraction{}, err
	}
	interactionRuntime, ok := runtime.(InteractionRuntime)
	if !ok {
		return preparedPromptInteraction{}, fmt.Errorf("%w: prompt attach is unsupported by this runtime driver", domain.ErrUnsupported)
	}
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		return preparedPromptInteraction{}, err
	}
	run, err = markProjectRunInteractionArtifacts(ctx, runCtx, logsPath, artifactsDir)
	if err != nil {
		return preparedPromptInteraction{}, err
	}
	agentConfig, err := c.projectRunAgentConfig(ctx, run)
	if err != nil {
		return preparedPromptInteraction{}, err
	}
	if agentConfig.Provider != "codex" && agentConfig.Provider != "claude" && agentConfig.Provider != "opencode" && agentConfig.Provider != "pi" {
		return preparedPromptInteraction{}, fmt.Errorf("%w: prompt attach currently supports codex, claude, opencode, and pi providers only", domain.ErrUnsupported)
	}
	systemPrompt, err := c.projectRunAgentSystemPrompt(ctx, run)
	if err != nil {
		return preparedPromptInteraction{}, err
	}
	if err := execution.WriteAgentSystemPromptFile(sandbox, systemPrompt); err != nil {
		return preparedPromptInteraction{}, err
	}
	schemaPath, err := execution.WriteAgentOutputSchemaFile(c.config, sandbox, agentConfig.Provider, req.OutputSchemaJSON)
	if err != nil {
		return preparedPromptInteraction{}, err
	}
	env := execution.BuildSandboxExecEnv(c.config, sandbox, c.config.GuestHomePath)
	managedEnv, err := c.ensurePromptAttachLLMFacadeEnv(ctx, sandbox, agentConfig, run.RunID)
	if err != nil {
		return preparedPromptInteraction{}, err
	}
	if len(managedEnv) > 0 {
		env = llms.MergeManagedExecEnv(env, managedEnv)
	}
	return preparedPromptInteraction{
		Run:                run,
		LogsPath:           logsPath,
		AgentConfig:        agentConfig,
		SchemaPath:         schemaPath,
		Env:                env,
		ManagedEnv:         managedEnv,
		VMState:            vmState,
		InteractionRuntime: interactionRuntime,
	}, nil
}

func (c *Controller) runPromptInteraction(ctx context.Context, runCtx interactionRunContext, _ RunAttachInput, receive RunAttachReceiver, send RunAttachSender) (transitionResult TransitionRequest, returnErr error) {
	run := runCtx.Run
	sandbox := runCtx.Sandbox
	prepared, err := c.preparePromptInteractionRuntime(ctx, runCtx)
	if err != nil {
		logsPath := filepath.Join(projectRunCommandArtifactsDir(run, sandbox), "transcript.txt")
		return TransitionRequest{RunID: run.RunID, SandboxID: sandbox.Summary.ID, LogsPath: logsPath, ExitCode: 1, Error: fmt.Sprintf("agent execution failed: %v", err)}, err
	}
	if token := prepared.ManagedEnv["AGENT_COMPOSE_SANDBOX_TOKEN"]; token != "" {
		defer func() {
			if !errors.Is(returnErr, domain.ErrExecTerminationUnconfirmed) && !errors.Is(returnErr, driverpkg.ErrExecTerminationUnconfirmed) {
				c.deletePromptAttachLLMFacadeToken(context.WithoutCancel(ctx), token)
			}
		}()
	}
	return c.runPromptInteractionSession(ctx, runCtx, prepared, receive, send)
}

func (c *Controller) runPromptInteractionSession(ctx context.Context, runCtx interactionRunContext, prepared preparedPromptInteraction, receive RunAttachReceiver, send RunAttachSender) (TransitionRequest, error) {
	session, err := c.openPromptInteractionSession(ctx, runCtx, prepared)
	if err != nil {
		return TransitionRequest{RunID: prepared.Run.RunID, SandboxID: runCtx.Sandbox.Summary.ID, LogsPath: prepared.LogsPath, ExitCode: 1, Error: fmt.Sprintf("agent execution failed: %v", err)}, err
	}
	defer func() { _ = session.Interaction.CloseSend() }()
	inputCtx, cancelInput := context.WithCancel(ctx)
	defer cancelInput()
	onHumanMessage := func(message, clientMessageID string) error {
		if err := session.Projector.AppendHumanMessageFrame(message, clientMessageID); err != nil {
			return err
		}
		if runCtx.Coordinator == nil {
			return fmt.Errorf("prompt resume transition requires coordinator")
		}
		// The message event retains its own idempotency boundary. The companion
		// state transition records that this persisted reply has resumed work.
		_, err := runCtx.Coordinator.MarkRunning(context.WithoutCancel(ctx), session.Run.RunID, session.Sandbox.Summary.ID)
		return err
	}
	go pumpRunPromptAttachInput(inputCtx, receive, promptInputPump{Input: session.Input, TurnReady: session.TurnReady, OnHumanMessage: onHumanMessage})
	return receivePromptInteractionFrames(ctx, session.Run, session.Sandbox, session.Transition, promptInteractionReceiveState{
		Interaction: session.Interaction,
		Projector:   session.Projector,
		Coordinator: runCtx.Coordinator,
		TurnReady:   session.TurnReady,
	}, send)
}

// promptInteractionSession owns one initialized runtime stream. It is shared
// by the traditional attach path and the daemon-owned conversation manager so
// both negotiate and project exactly the same wire protocol.
type promptInteractionSession struct {
	Run          domain.ProjectRunRecord
	Sandbox      *domain.Sandbox
	Transition   TransitionRequest
	Interaction  driverpkg.RuntimeInteraction
	Projector    *promptAttachProjector
	Input        *promptWrapperInput
	TurnReady    chan struct{}
	ManagedToken string
}

// openPromptInteractionSession opens the runtime stream and sends its initial
// protocol frames. Its caller owns closing the interaction input when the
// runtime ends, which lets detached conversations outlive HTTP observers.
func (c *Controller) openPromptInteractionSession(ctx context.Context, runCtx interactionRunContext, prepared preparedPromptInteraction) (promptInteractionSession, error) {
	run := prepared.Run
	sandbox := runCtx.Sandbox
	logsPath := prepared.LogsPath
	transition := TransitionRequest{RunID: run.RunID, SandboxID: sandbox.Summary.ID, LogsPath: logsPath}
	command := strings.Join([]string{
		"set -e",
		"cd " + execution.ShellQuote(c.config.GuestWorkspacePath),
		"mkdir -p " + execution.ShellQuote(c.config.GuestHomePath),
		"agent-compose-runtime stream",
	}, " && ")
	spec := driverpkg.RuntimeStartSpec{
		OperationID: run.RunID,
		Kind:        driverpkg.RuntimeOperationCommand,
		Origin:      "run_prompt_attach",
		Command: &driverpkg.RuntimeCommandSpec{
			Command: "sh",
			Args:    []string{"-lc", command},
			Env:     prepared.Env,
			Cwd:     c.config.GuestWorkspacePath,
		},
		Cwd:         c.config.GuestWorkspacePath,
		Env:         prepared.Env,
		AttachStdin: true,
		TTY:         false,
	}
	interaction, err := prepared.InteractionRuntime.OpenInteraction(ctx, sandbox, prepared.VMState, spec)
	if err != nil {
		return promptInteractionSession{}, err
	}
	interaction = driverpkg.GuardRuntimeInteractionInput(interaction)
	projector := newPersistentPromptAttachProjector(context.WithoutCancel(ctx), persistentPromptAttachProjectorDeps{Run: run, Sandbox: sandbox, LogsPath: logsPath, Hub: c.runLogs, EventStore: c.configDB})
	input := &promptWrapperInput{interaction: interaction}
	if err := input.Start(prepared.AgentConfig, c.config, prepared.SchemaPath); err != nil {
		_ = interaction.CloseSend()
		return promptInteractionSession{}, err
	}
	turnReady := make(chan struct{}, 1)
	if prompt := strings.TrimSpace(runCtx.Request.Prompt); prompt != "" {
		if err := input.HumanMessage(prompt); err != nil {
			_ = interaction.CloseSend()
			return promptInteractionSession{}, err
		}
	} else {
		releasePromptTurn(turnReady)
	}
	return promptInteractionSession{
		Run: run, Sandbox: sandbox, Transition: transition, Interaction: interaction, Projector: projector, Input: input, TurnReady: turnReady,
		ManagedToken: prepared.ManagedEnv["AGENT_COMPOSE_SANDBOX_TOKEN"],
	}, nil
}

// promptInteractionReceiveState bundles the open interaction, log
// projector, and turn-ready gate receivePromptInteractionFrames needs as it
// pumps frames from the runtime until the interaction ends.
type promptInteractionReceiveState struct {
	Interaction driverpkg.RuntimeInteraction
	Projector   *promptAttachProjector
	Coordinator *Coordinator
	TurnReady   chan struct{}
}

func receivePromptInteractionFrames(ctx context.Context, run domain.ProjectRunRecord, sandbox *domain.Sandbox, transition TransitionRequest, state promptInteractionReceiveState, send RunAttachSender) (TransitionRequest, error) {
	interaction, projector, turnReady := state.Interaction, state.Projector, state.TurnReady
	logsPath := transition.LogsPath
	var promptTransition *TransitionRequest
	for {
		frame, err := interaction.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				result, waitErr := interaction.Wait()
				if waitErr != nil {
					transition.ExitCode = 1
					transition.Error = fmt.Sprintf("agent execution failed: %v", waitErr)
					return transition, waitErr
				}
				if promptTransition != nil {
					if result.ExitCode != 0 || !result.Success {
						promptTransition.ExitCode = execution.FirstNonZeroInt(result.ExitCode, promptTransition.ExitCode)
						promptTransition.Error = firstNonEmpty(promptTransition.Error, result.Error, "agent execution failed")
						return *promptTransition, promptWrapperTransitionError(*promptTransition)
					}
					if promptTransition.ExitCode != 0 || strings.TrimSpace(promptTransition.Error) != "" {
						return *promptTransition, promptWrapperTransitionError(*promptTransition)
					}
					return *promptTransition, nil
				}
				return transitionFromPromptRuntimeResult(run, sandbox, promptRuntimeOutcome{LogsPath: logsPath, Result: result, Err: errorFromRuntimeResult(result)}), errorFromRuntimeResult(result)
			}
			transition.ExitCode = 1
			transition.Error = fmt.Sprintf("agent execution failed: %v", err)
			_ = send(runAttachErrorResponse("runtime_recv_error", err.Error(), true))
			return transition, err
		}
		switch frame.Type {
		case driverpkg.RuntimeOutputStarted:
			if err := send(runAttachStartedResponse(run, sandbox, warningsFromRun(run), frame.StartedAt)); err != nil {
				transition.ExitCode = 1
				transition.Error = fmt.Sprintf("agent execution failed: %v", err)
				return transition, err
			}
		case driverpkg.RuntimeOutputStdout:
			responses, nextTransition, err := projector.Project(frame.Data)
			if err != nil {
				_ = send(runAttachErrorResponse("runtime_stream_decode_error", err.Error(), true))
				transition.ExitCode = 1
				transition.Error = fmt.Sprintf("agent execution failed: %v", err)
				return transition, err
			}
			turnCompleted := false
			closeInputAfterTurn := false
			for _, resp := range responses {
				if err := send(resp); err != nil {
					transition.ExitCode = 1
					transition.Error = fmt.Sprintf("agent execution failed: %v", err)
					return transition, err
				}
				if resp.Kind == RunAttachOutputAgentTurnCompleted {
					turnCompleted = true
				}
			}
			if nextTransition != nil {
				promptTransition = nextTransition
				if nextTransition.Status == domain.ProjectRunStatusWaitingForInput {
					if state.Coordinator == nil {
						return transition, fmt.Errorf("prompt waiting transition requires coordinator")
					}
					// A runtime can remain attached while awaiting the next human
					// message. Persist the pause now, with its assistant/status
					// events, rather than deferring it until the runtime exits.
					if _, err := state.Coordinator.TransitionRun(context.WithoutCancel(ctx), *nextTransition); err != nil {
						return transition, err
					}
				} else if nextTransition.Status == domain.ProjectRunStatusSucceeded {
					// A completed turn is terminal, but the runtime keeps its stream
					// open for further human messages until stdin reaches EOF. Close
					// only the input half so it can emit its final result frame and
					// let the existing terminal-transition path finish the run.
					closeInputAfterTurn = true
				}
			}
			if closeInputAfterTurn {
				if err := interaction.CloseSend(); err != nil {
					transition.ExitCode = 1
					transition.Error = fmt.Sprintf("close completed conversation input: %v", err)
					return transition, err
				}
			}
			// Do not let the input pump send a reply until a needs-input turn has
			// durably entered waiting_for_input. The next human message can then
			// atomically observe and resume that exact lifecycle edge.
			if turnCompleted && !closeInputAfterTurn {
				releasePromptTurn(turnReady)
			}
		case driverpkg.RuntimeOutputStderr:
			if err := projector.AppendStderr(string(frame.Data)); err != nil {
				transition.ExitCode = 1
				transition.Error = fmt.Sprintf("agent execution failed: %v", err)
				return transition, err
			}
			if err := send(runAttachOutputResponse(frame.Data, domain.StdioStderr, false)); err != nil {
				transition.ExitCode = 1
				transition.Error = fmt.Sprintf("agent execution failed: %v", err)
				return transition, err
			}
		case driverpkg.RuntimeOutputResult:
			result := frame.Result
			if result == nil {
				result = &driverpkg.RuntimeResult{OperationID: run.RunID, Success: true}
			}
			if promptTransition != nil {
				if result.ExitCode != 0 || !result.Success {
					promptTransition.ExitCode = execution.FirstNonZeroInt(result.ExitCode, promptTransition.ExitCode)
					promptTransition.Error = firstNonEmpty(promptTransition.Error, result.Error, "agent execution failed")
					return *promptTransition, promptWrapperTransitionError(*promptTransition)
				}
				if promptTransition.ExitCode != 0 || strings.TrimSpace(promptTransition.Error) != "" {
					return *promptTransition, promptWrapperTransitionError(*promptTransition)
				}
				return *promptTransition, nil
			}
			return transitionFromPromptRuntimeResult(run, sandbox, promptRuntimeOutcome{LogsPath: logsPath, Result: *result, Err: errorFromRuntimeResult(*result)}), errorFromRuntimeResult(*result)
		case driverpkg.RuntimeOutputError:
			code := "runtime_error"
			message := "runtime interaction failed"
			if frame.Error != nil {
				code = firstNonEmpty(frame.Error.Code, code)
				message = firstNonEmpty(frame.Error.Message, message)
			}
			_ = send(runAttachErrorResponse(code, message, true))
			transition.ExitCode = 1
			transition.Error = message
			return transition, errors.New(message)
		}
	}
}

type promptWrapperInput struct {
	interaction driverpkg.RuntimeInteraction
	seq         int
}

func (w *promptWrapperInput) Start(agent execution.AgentConfig, config *appconfig.Config, schemaPath string) error {
	// Negotiate the framed interaction contract so the runtime emits explicit
	// turnState values that the durable prompt projector can interpret.
	frame := map[string]any{
		"v":                   1,
		"seq":                 w.nextSeq(),
		"type":                "start",
		"interactionProtocol": "agent-compose.v1",
		"provider":            agent.Provider,
		"stateRoot":           config.GuestStateRoot,
		"workspace":           config.GuestWorkspacePath,
		"home":                config.GuestHomePath,
	}
	if strings.TrimSpace(agent.Model) != "" {
		frame["model"] = strings.TrimSpace(agent.Model)
	}
	if strings.TrimSpace(schemaPath) != "" {
		frame["outputSchemaFile"] = strings.TrimSpace(schemaPath)
	}
	return w.send(frame)
}

func (w *promptWrapperInput) HumanMessage(message string) error {
	return w.send(map[string]any{
		"v":       1,
		"seq":     w.nextSeq(),
		"type":    "human_message",
		"message": message,
	})
}

func (w *promptWrapperInput) EOF() error {
	return w.send(map[string]any{"v": 1, "seq": w.nextSeq(), "type": "eof"})
}

func (w *promptWrapperInput) Cancel(reason string) error {
	return w.send(map[string]any{"v": 1, "seq": w.nextSeq(), "type": "cancel", "message": reason})
}

func (w *promptWrapperInput) nextSeq() int {
	seq := w.seq
	w.seq++
	return seq
}

func (w *promptWrapperInput) send(frame map[string]any) error {
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return w.interaction.Send(driverpkg.RuntimeInputFrame{Type: driverpkg.RuntimeInputStdin, Data: data})
}

// promptInputPump groups the wrapper input stream, the gate that releases a
// turn, and the callback notified of each forwarded human message.
type promptInputPump struct {
	Input          *promptWrapperInput
	TurnReady      <-chan struct{}
	OnHumanMessage func(string, string) error
}

func pumpRunPromptAttachInput(ctx context.Context, receive RunAttachReceiver, pump promptInputPump) {
	defer func() { _ = pump.Input.interaction.CloseSend() }()
	for {
		req, err := receive()
		if err != nil {
			_ = pump.Input.EOF()
			return
		}
		switch req.Kind {
		case RunAttachInputHumanMessage:
			if !forwardPromptHumanMessage(ctx, pump, req.Text, req.ClientFrameID) {
				return
			}
		case RunAttachInputStdin:
			if !forwardPromptHumanMessage(ctx, pump, string(req.Data), req.ClientFrameID) {
				return
			}
		case RunAttachInputStdinEOF:
			_ = pump.Input.EOF()
			return
		case RunAttachInputCancel:
			_ = pump.Input.Cancel(req.Reason)
			return
		default:
			_ = pump.Input.Cancel("invalid run prompt attach frame")
			return
		}
	}
}

func forwardPromptHumanMessage(ctx context.Context, pump promptInputPump, text, clientFrameID string) bool {
	if pump.TurnReady != nil {
		select {
		case <-ctx.Done():
			return false
		case <-pump.TurnReady:
		}
	}
	if pump.OnHumanMessage != nil {
		if err := pump.OnHumanMessage(text, clientFrameID); err != nil {
			return false
		}
	}
	return pump.Input.HumanMessage(text) == nil
}

func releasePromptTurn(turnReady chan<- struct{}) {
	select {
	case turnReady <- struct{}{}:
	default:
	}
}
