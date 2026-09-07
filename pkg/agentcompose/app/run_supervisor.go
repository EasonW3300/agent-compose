package app

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/samber/do/v2"

	domain "agent-compose/pkg/model"
	"agent-compose/pkg/runs"
	"agent-compose/pkg/storage/configstore"
)

type RunSupervisor struct {
	root          context.Context
	controller    *runs.Controller
	store         *configstore.ConfigStore
	conversations *runs.ConversationManager

	mu     sync.Mutex
	active map[string]*activeRun
	wg     sync.WaitGroup
}

type activeRun struct {
	cancel   context.CancelCauseFunc
	stopOnce sync.Once
	stopping bool
	stopped  bool
	stopErr  error
}

func NewRunSupervisor(di do.Injector) (*RunSupervisor, error) {
	root := do.MustInvoke[context.Context](di)
	controller := do.MustInvoke[*runs.Controller](di)
	conversations := runs.NewConversationManager(runs.ConversationManagerDeps{
		Root:       root,
		Controller: controller,
	})
	// A live runtime stream cannot survive a daemon restart. Resolve durable
	// prompt conversations before this supervisor can expose new reply handlers.
	if err := conversations.RecoverInterrupted(root); err != nil {
		return nil, err
	}
	return &RunSupervisor{
		root:          root,
		controller:    controller,
		store:         do.MustInvoke[*configstore.ConfigStore](di),
		conversations: conversations,
		active:        map[string]*activeRun{},
	}, nil
}

func (s *RunSupervisor) StartRun(ctx context.Context, req runs.RunAgentRequest) (domain.ProjectRunRecord, error) {
	started, err := s.controller.StartProjectRun(ctx, req)
	if err != nil {
		return domain.ProjectRunRecord{}, err
	}
	if runs.StatusIsTerminal(started.Run.Status) {
		return started.Run, nil
	}
	execCtx, cancel := context.WithCancelCause(s.root)
	s.register(started.Run.RunID, cancel)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.unregister(started.Run.RunID)
		_, _, _ = started.Execute(execCtx, nil)
	}()
	return started.Run, nil
}

func (s *RunSupervisor) Run(ctx context.Context, req runs.RunAgentRequest, stream *runs.StreamSink) (domain.ProjectRunRecord, error, error) {
	started, err := s.controller.StartProjectRun(ctx, req)
	if err != nil {
		return domain.ProjectRunRecord{}, nil, err
	}
	if runs.StatusIsTerminal(started.Run.Status) {
		return started.Run, nil, nil
	}
	execCtx, cancel := context.WithCancelCause(ctx)
	s.register(started.Run.RunID, cancel)
	s.wg.Add(1)
	defer s.wg.Done()
	defer s.unregister(started.Run.RunID)
	return started.Execute(execCtx, stream)
}

func (s *RunSupervisor) Attach(ctx context.Context, receive runs.RunAttachReceiver, send runs.RunAttachSender) error {
	execCtx, cancel := context.WithCancelCause(ctx)
	var runID string
	err := s.controller.RunProjectCommandAttachRegistered(execCtx, receive, send, func(startedRunID string) {
		runID = startedRunID
		s.register(runID, cancel)
		s.wg.Add(1)
	})
	if runID != "" {
		s.wg.Done()
		s.unregister(runID)
	} else {
		cancel(nil)
	}
	return err
}

func (s *RunSupervisor) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	// Stop detached prompt runtimes first. Their persisted state is retained so
	// the next daemon startup can mark the interrupted conversations explicitly.
	if err := s.conversations.Shutdown(ctx); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *RunSupervisor) StopActiveRun(ctx context.Context, runID, reason string) (bool, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return false, nil
	}
	s.mu.Lock()
	active, ok := s.active[runID]
	if ok {
		active.stopping = true
	}
	s.mu.Unlock()
	if !ok {
		return false, nil
	}
	active.stopOnce.Do(func() {
		reason = strings.TrimSpace(reason)
		if reason == "" {
			reason = "stop requested"
		}
		active.cancel(errors.New(reason))
		active.stopped = true
	})
	return active.stopped, active.stopErr
}

func (s *RunSupervisor) register(runID string, cancel context.CancelCauseFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active[runID] = &activeRun{cancel: cancel}
}

func (s *RunSupervisor) unregister(runID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active, runID)
}
