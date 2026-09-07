package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	domain "agent-compose/pkg/model"
)

// json decodes runtime frames and encodes durable client-message metadata;
// domain supplies the run/event records projected from those untrusted frames.

type promptAttachProjector struct {
	run                    domain.ProjectRunRecord
	sandbox                *domain.Sandbox
	logsPath               string
	runLogs                *RunLogHub
	mu                     sync.Mutex
	buffer                 []byte
	itemTexts              map[string]string
	loggedText             string
	turnText               string
	hasLoggedText          bool
	logEndsWithNewline     bool
	persistedAssistantTurn bool
	eventCtx               context.Context
	events                 structuredEventStore
	humanIndex             uint64
}

// persistentPromptAttachProjectorDeps groups the dependencies needed to build
// a promptAttachProjector that also persists structured events.
type persistentPromptAttachProjectorDeps struct {
	Run        domain.ProjectRunRecord
	Sandbox    *domain.Sandbox
	LogsPath   string
	Hub        *RunLogHub
	EventStore any
}

func newPersistentPromptAttachProjector(ctx context.Context, deps persistentPromptAttachProjectorDeps) *promptAttachProjector {
	projector := newPromptAttachProjector(deps.Run, deps.Sandbox, deps.LogsPath, deps.Hub)
	projector.eventCtx = ctx
	projector.events, _ = deps.EventStore.(structuredEventStore)
	return projector
}

func newPromptAttachProjector(run domain.ProjectRunRecord, sandbox *domain.Sandbox, logsPath string, hub *RunLogHub) *promptAttachProjector {
	return &promptAttachProjector{
		run:       run,
		sandbox:   sandbox,
		logsPath:  logsPath,
		runLogs:   hub,
		itemTexts: map[string]string{},
	}
}

func (p *promptAttachProjector) Project(data []byte) ([]RunAttachOutput, *TransitionRequest, error) {
	p.buffer = append(p.buffer, data...)
	lines := make([][]byte, 0)
	for {
		index := bytesIndexByte(p.buffer, '\n')
		if index < 0 {
			break
		}
		line := append([]byte(nil), p.buffer[:index]...)
		p.buffer = append([]byte(nil), p.buffer[index+1:]...)
		if strings.TrimSpace(string(line)) != "" {
			lines = append(lines, line)
		}
	}
	var responses []RunAttachOutput
	var transition *TransitionRequest
	for _, line := range lines {
		nextResponses, nextTransition, err := p.projectLine(line)
		if err != nil {
			return responses, transition, err
		}
		responses = append(responses, nextResponses...)
		if nextTransition != nil {
			transition = nextTransition
		}
	}
	return responses, transition, nil
}

func (p *promptAttachProjector) projectLine(line []byte) ([]RunAttachOutput, *TransitionRequest, error) {
	var frame struct {
		Type            string                      `json:"type"`
		Event           json.RawMessage             `json:"event"`
		FinalText       string                      `json:"finalText"`
		FinalTextSource domain.AgentFinalTextSource `json:"finalTextSource"`
		SandboxID       string                      `json:"sandboxId"`
		StopReason      string                      `json:"stopReason"`
		Code            string                      `json:"code"`
		Message         string                      `json:"message"`
		Provider        string                      `json:"provider"`
		Seq             uint64                      `json:"seq"`
		TurnState       string                      `json:"turnState"`
	}
	if err := json.Unmarshal(line, &frame); err != nil {
		return nil, nil, err
	}
	switch frame.Type {
	case "started":
		return []RunAttachOutput{runAttachAgentEventResponse("started", "", string(line))}, nil, nil
	case "agent_event":
		name, text := p.agentEventText(frame.Event)
		if err := p.appendLogText(text); err != nil {
			return nil, nil, err
		}
		return []RunAttachOutput{runAttachAgentEventResponse(firstNonEmpty(name, "agent_event"), text, string(frame.Event))}, nil, nil
	case "agent_turn_completed":
		if err := p.appendLogFinalText(frame.FinalText); err != nil {
			return nil, nil, err
		}
		turn := agentTurnProjection{FinalText: frame.FinalText, FinalTextSource: frame.FinalTextSource, Provider: frame.Provider, StopReason: frame.StopReason}
		if transition, hasTurnState, err := p.transitionFromTurnState(line, frame.Seq, frame.TurnState, turn); err != nil {
			return nil, nil, err
		} else if hasTurnState {
			return []RunAttachOutput{runAttachAgentTurnCompletedResponse(p.run, string(line), warningsFromRun(p.run))}, transition, nil
		}
		if err := p.appendAssistantEvent(line, frame.Seq, turn); err != nil {
			return nil, nil, err
		}
		return []RunAttachOutput{runAttachAgentTurnCompletedResponse(p.run, string(line), warningsFromRun(p.run))}, nil, nil
	case "result":
		if err := p.appendLogFinalText(frame.FinalText); err != nil {
			return nil, nil, err
		}
		transition := transitionFromPromptWrapperResult(p.run, p.sandbox, promptWrapperOutcome{LogsPath: p.logsPath, Payload: line, FinalText: frame.FinalText, StopReason: frame.StopReason})
		transition.TerminalEvents = p.terminalTurnEvents(agentTurnProjection{FinalText: frame.FinalText, FinalTextSource: frame.FinalTextSource, Provider: frame.Provider, StopReason: frame.StopReason})
		return nil, &transition, nil
	case "error":
		message := firstNonEmpty(frame.Message, "runtime stream error")
		transition := transitionFromPromptWrapperResult(p.run, p.sandbox, promptWrapperOutcome{LogsPath: p.logsPath, Payload: line, StopReason: frame.StopReason, Message: message})
		return []RunAttachOutput{runAttachErrorResponse(firstNonEmpty(frame.Code, "runtime_stream_error"), message, true)}, &transition, nil
	default:
		return []RunAttachOutput{runAttachAgentEventResponse(firstNonEmpty(frame.Type, "agent_event"), "", string(line))}, nil, nil
	}
}

func (p *promptAttachProjector) transitionFromTurnState(line []byte, sequence uint64, turnState string, turn agentTurnProjection) (*TransitionRequest, bool, error) {
	switch strings.TrimSpace(turnState) {
	case "":
		return nil, false, nil
	case "needs_input", "completed":
		transition := transitionFromPromptWrapperResult(p.run, p.sandbox, promptWrapperOutcome{
			LogsPath: p.logsPath, Payload: line, FinalText: turn.FinalText, StopReason: turn.StopReason,
		})
		// Keep the assistant message in the coordinator's transaction with the
		// state event, rather than committing a visible question independently.
		transition.TerminalEvents = p.turnEvents(line, sequence, turn)
		if strings.TrimSpace(turnState) == "needs_input" {
			transition.Status = domain.ProjectRunStatusWaitingForInput
		} else {
			transition.Status = domain.ProjectRunStatusSucceeded
		}
		return &transition, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported interactive turn state %q", turnState)
	}
}

func (p *promptAttachProjector) agentEventText(raw json.RawMessage) (string, string) {
	var event struct {
		Type string `json:"type"`
		Text string `json:"text"`
		Item *struct {
			ID               string `json:"id"`
			Type             string `json:"type"`
			Text             string `json:"text"`
			AggregatedOutput string `json:"aggregated_output"`
			Command          string `json:"command"`
		} `json:"item"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return "agent_event", ""
	}
	name := firstNonEmpty(event.Type, "agent_event")
	if event.Text != "" {
		return name, event.Text
	}
	if event.Item == nil {
		return name, ""
	}
	key := firstNonEmpty(event.Item.ID, name)
	var text string
	switch event.Item.Type {
	case "agent_message", "reasoning":
		text = event.Item.Text
	case "command_execution":
		if event.Item.Command != "" {
			commandKey := key + ":command"
			if p.itemTexts[commandKey] == "" {
				p.itemTexts[commandKey] = event.Item.Command
				text += "\n$ " + event.Item.Command + "\n"
			}
		}
		text += event.Item.AggregatedOutput
	default:
		return name, ""
	}
	if text == "" {
		return name, ""
	}
	previous := p.itemTexts[key]
	p.itemTexts[key] = text
	if strings.HasPrefix(text, previous) {
		return name, text[len(previous):]
	}
	return name, text
}

func (p *promptAttachProjector) appendLogText(text string) error {
	if text == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.appendLogChunkLocked(domain.ExecChunk{Text: text}); err != nil {
		return err
	}
	p.loggedText += text
	p.turnText += text
	return nil
}

func (p *promptAttachProjector) appendLogFinalText(finalText string) error {
	if finalText == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if strings.HasPrefix(finalText, p.loggedText) {
		text := finalText[len(p.loggedText):]
		if text == "" {
			return nil
		}
		if err := p.appendLogChunkLocked(domain.ExecChunk{Text: text}); err != nil {
			return err
		}
		p.loggedText += text
		p.turnText += text
		return nil
	}
	if p.loggedText == "" {
		if err := p.appendLogChunkLocked(domain.ExecChunk{Text: finalText}); err != nil {
			return err
		}
		p.loggedText = finalText
		p.turnText += finalText
	}
	return nil
}

func (p *promptAttachProjector) AppendHumanMessage(message string) error {
	return p.AppendHumanMessageFrame(message, "")
}

func (p *promptAttachProjector) AppendHumanMessageFrame(message, clientMessageID string) error {
	text := promptAttachHumanLogText(message)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.persistedAssistantTurn = false
	if text != "" {
		if p.hasLoggedText && !p.logEndsWithNewline {
			text = "\n" + text
		}
		if err := p.appendLogChunkLocked(domain.ExecChunk{Text: text}); err != nil {
			return err
		}
	}
	if p.events == nil || strings.TrimSpace(message) == "" {
		return nil
	}
	p.humanIndex++
	_, _, err := p.events.AppendProjectRunEvent(p.eventContext(), domain.ProjectRunEventRecord{
		ID: attachedHumanEventID(p.run.RunID, clientMessageID, uint64(p.humanIndex), message), RunID: p.run.RunID, Kind: domain.ProjectRunEventKindUserMessage, Text: message, Agent: p.run.AgentName,
		PayloadJSON: projectRunUserMessagePayload(clientMessageID),
	})
	return err
}

func (p *promptAttachProjector) AppendStderr(text string) error {
	if text == "" {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.appendLogChunkLocked(domain.ExecChunk{Text: text, Stream: domain.StdioStderr}); err != nil {
		return err
	}
	p.turnText += text
	return nil
}

func (p *promptAttachProjector) appendAssistantEvent(line []byte, seq uint64, turn agentTurnProjection) error {
	p.mu.Lock()
	store := p.events
	turn.Transcript = p.turnText
	p.mu.Unlock()
	if store == nil {
		return nil
	}
	events := attachedAgentTurnEvents(p.run, seq, line, turn)
	if len(events) > 0 {
		if _, _, err := store.AppendProjectRunEvents(p.eventContext(), events); err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.turnText = ""
	p.persistedAssistantTurn = len(events) > 0
	return nil
}

func (p *promptAttachProjector) turnEvents(line []byte, sequence uint64, turn agentTurnProjection) []domain.ProjectRunEventRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	turn.Transcript = p.turnText
	events := attachedAgentTurnEvents(p.run, sequence, line, turn)
	p.turnText = ""
	p.persistedAssistantTurn = len(events) > 0
	return events
}

func (p *promptAttachProjector) terminalTurnEvents(turn agentTurnProjection) []domain.ProjectRunEventRecord {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.persistedAssistantTurn {
		return nil
	}
	turn.Transcript = p.turnText
	events := terminalPromptTurnEvents(p.run, turn)
	p.turnText = ""
	return events
}

func (p *promptAttachProjector) eventContext() context.Context {
	if p.eventCtx != nil {
		return p.eventCtx
	}
	return context.Background()
}

func (p *promptAttachProjector) appendLogChunkLocked(chunk domain.ExecChunk) error {
	offset, err := appendProjectRunLogChunk(p.logsPath, chunk)
	if err != nil {
		return err
	}
	if chunk.Text != "" {
		p.hasLoggedText = true
		p.logEndsWithNewline = strings.HasSuffix(chunk.Text, "\n")
	}
	publishRunLogChunk(p.runLogs, p.run.RunID, chunk, offset)
	return nil
}

func promptAttachHumanLogText(message string) string {
	if strings.TrimSpace(message) == "" {
		return ""
	}
	if strings.HasSuffix(message, "\n") {
		return message
	}
	return message + "\n"
}

func projectRunUserMessagePayload(clientMessageID string) string {
	clientMessageID = strings.TrimSpace(clientMessageID)
	if clientMessageID == "" {
		return ""
	}
	payload, err := json.Marshal(struct {
		ClientMessageID string `json:"client_message_id"`
	}{ClientMessageID: clientMessageID})
	if err != nil {
		return ""
	}
	return string(payload)
}

func bytesIndexByte(data []byte, needle byte) int {
	for i, value := range data {
		if value == needle {
			return i
		}
	}
	return -1
}
