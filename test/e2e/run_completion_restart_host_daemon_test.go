package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	containerapi "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"

	agentcomposev2 "agent-compose/proto/agentcompose/v2"
	"agent-compose/proto/agentcompose/v2/agentcomposev2connect"
)

const dockerRunCompletionE2EImageEnv = "AGENT_COMPOSE_E2E_RUN_COMPLETION_IMAGE"

func TestInteractiveRunWaitsThenCompletesAfterReply(t *testing.T) {
	runInteractiveConversationLifecycleE2E(t)
}

// runInteractiveConversationLifecycleE2E drives the daemon through the public
// Connect boundary. The prompt requires a human-supplied path, so the durable
// runtime stream must remain available while the run waits for that reply.
func runInteractiveConversationLifecycleE2E(t *testing.T) {
	t.Helper()
	image := strings.TrimSpace(os.Getenv(dockerRunCompletionE2EImageEnv))
	if image == "" {
		t.Skipf("set %s to a local Docker guest image", dockerRunCompletionE2EImageEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	repoRoot := e2eRepoRoot(t)
	testRoot := newDockerVisibleE2ETempRoot(t, "interactive-conversation-")
	dockerClient := newE2EDockerClient(t, ctx, image)
	binary := e2eDaemonBinary(t, ctx, repoRoot, testRoot)
	listenAddress := unusedLoopbackAddress(t)
	baseURL := "http://" + listenAddress
	daemon := startE2EDaemonWithEnv(t, binary, repoRoot, testRoot, listenAddress, image, map[string]string{
		"AGENT_COMPOSE_RUNTIME_BASE_URL": dockerDesktopRuntimeBaseURL(t, listenAddress),
	})
	waitForE2EDaemon(t, ctx, daemon, baseURL)

	httpClient := newE2EHTTPClient()
	projectClient := agentcomposev2connect.NewProjectServiceClient(httpClient, baseURL)
	runClient := agentcomposev2connect.NewRunServiceClient(httpClient, baseURL)
	projectID := applyE2ERunCompletionProject(t, ctx, projectClient, testRoot, image)

	started, err := runClient.StartInteractiveAgentRun(ctx, connect.NewRequest(&agentcomposev2.StartAgentRunRequest{
		Run: &agentcomposev2.RunAgentRequest{
			ProjectId:       projectID,
			AgentName:       "worker",
			Prompt:          "Ask the human for the required work-log path before doing any work. After the human replies with a path, confirm the work is complete.",
			Source:          agentcomposev2.RunSource_RUN_SOURCE_API,
			ClientRequestId: "docker-interactive-conversation-start",
		},
	}))
	if err != nil {
		t.Fatalf("StartInteractiveAgentRun returned error: %v\ndaemon log:\n%s", err, daemon.logs.String())
	}
	runID := started.Msg.GetRun().GetRunId()
	if runID == "" || !started.Msg.GetStarted() {
		t.Fatalf("StartInteractiveAgentRun response = %#v, want newly started run", started.Msg)
	}

	var waiting *agentcomposev2.RunDetail
	waitForE2ECondition(t, 2*time.Minute, func() bool {
		response, getErr := runClient.GetRun(ctx, connect.NewRequest(&agentcomposev2.GetRunRequest{RunId: runID}))
		if getErr != nil {
			return false
		}
		waiting = response.Msg.GetRun()
		summary := waiting.GetSummary()
		switch summary.GetStatus() {
		case agentcomposev2.RunStatus_RUN_STATUS_WAITING_FOR_INPUT:
			return true
		case agentcomposev2.RunStatus_RUN_STATUS_SUCCEEDED, agentcomposev2.RunStatus_RUN_STATUS_FAILED,
			agentcomposev2.RunStatus_RUN_STATUS_CANCELED, agentcomposev2.RunStatus_RUN_STATUS_TIMED_OUT,
			agentcomposev2.RunStatus_RUN_STATUS_INTERRUPTED:
			t.Fatalf("interactive run reached terminal status before human reply: status=%s error=%q\ndaemon log:\n%s", summary.GetStatus(), summary.GetError(), daemon.logs.String())
		}
		return false
	}, "interactive run did not wait for human input")
	if sandboxID := waiting.GetSummary().GetSandboxId(); sandboxID != "" {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			removeE2EDockerSandboxFallback(t, cleanupCtx, dockerClient, sandboxID)
		})
	}

	replied, err := runClient.SendRunHumanMessage(ctx, connect.NewRequest(&agentcomposev2.SendRunHumanMessageRequest{
		RunId: runID, Text: "/workspace/work-log.md", ClientMessageId: "docker-interactive-conversation-reply",
	}))
	if err != nil {
		t.Fatalf("SendRunHumanMessage returned error: %v\ndaemon log:\n%s", err, daemon.logs.String())
	}
	if got := replied.Msg.GetRun().GetRunId(); got != runID {
		t.Fatalf("SendRunHumanMessage run ID = %q, want %q", got, runID)
	}

	var completed *agentcomposev2.RunDetail
	waitForE2ECondition(t, 2*time.Minute, func() bool {
		response, getErr := runClient.GetRun(ctx, connect.NewRequest(&agentcomposev2.GetRunRequest{RunId: runID}))
		if getErr != nil {
			return false
		}
		completed = response.Msg.GetRun()
		summary := completed.GetSummary()
		switch summary.GetStatus() {
		case agentcomposev2.RunStatus_RUN_STATUS_SUCCEEDED:
			return true
		case agentcomposev2.RunStatus_RUN_STATUS_FAILED, agentcomposev2.RunStatus_RUN_STATUS_CANCELED,
			agentcomposev2.RunStatus_RUN_STATUS_TIMED_OUT, agentcomposev2.RunStatus_RUN_STATUS_INTERRUPTED:
			t.Fatalf("interactive run did not complete after human reply: status=%s error=%q\ndaemon log:\n%s", summary.GetStatus(), summary.GetError(), daemon.logs.String())
		}
		return false
	}, "interactive run did not complete after human reply")
	if completed.GetSummary().GetCompletedAt() == nil {
		t.Fatalf("completed interactive run = %#v, want completed_at", completed)
	}
}

func TestE2EDockerRunCompletionRecoversAfterDaemonHardKill(t *testing.T) {
	image := strings.TrimSpace(os.Getenv(dockerRunCompletionE2EImageEnv))
	if image == "" {
		t.Skipf("set %s to a local Docker guest image", dockerRunCompletionE2EImageEnv)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	repoRoot := e2eRepoRoot(t)
	testRoot, err := os.MkdirTemp(repoRoot, ".docker-run-completion-e2e-")
	if err != nil {
		t.Fatalf("create Docker-visible E2E root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })
	dockerClient := newE2EDockerClient(t, ctx, image)
	binary := e2eDaemonBinary(t, ctx, repoRoot, testRoot)

	firstAddress := unusedLoopbackAddress(t)
	firstBaseURL := "http://" + firstAddress
	firstDaemon := startE2EDaemonWithEnv(t, binary, repoRoot, testRoot, firstAddress, image, map[string]string{
		"AGENT_COMPOSE_RUNTIME_BASE_URL": dockerDesktopRuntimeBaseURL(t, firstAddress),
	})
	waitForE2EDaemon(t, ctx, firstDaemon, firstBaseURL)
	firstHTTPClient := newE2EHTTPClient()
	projectClient := agentcomposev2connect.NewProjectServiceClient(firstHTTPClient, firstBaseURL)
	firstRunClient := agentcomposev2connect.NewRunServiceClient(firstHTTPClient, firstBaseURL)
	projectID := applyE2ERunCompletionProject(t, ctx, projectClient, testRoot, image)

	start, err := firstRunClient.StartAgentRun(ctx, connect.NewRequest(&agentcomposev2.StartAgentRunRequest{
		Run: &agentcomposev2.RunAgentRequest{
			ProjectId:       projectID,
			AgentName:       "worker",
			Command:         "printf run-started; sleep 300",
			Source:          agentcomposev2.RunSource_RUN_SOURCE_API,
			ClientRequestId: "docker-run-completion-hard-kill",
		},
	}))
	if err != nil {
		t.Fatalf("StartAgentRun returned error: %v\ndaemon log:\n%s", err, firstDaemon.logs.String())
	}
	runID := start.Msg.GetRun().GetRunId()
	if runID == "" || !start.Msg.GetStarted() {
		t.Fatalf("StartAgentRun response = %#v, want newly started run", start.Msg)
	}

	sandboxID := waitForE2ERunningRunSandbox(t, ctx, firstRunClient, dockerClient, runID)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		removeE2EDockerSandboxFallback(t, cleanupCtx, dockerClient, sandboxID)
	})
	firstDaemon.hardKill(t)
	assertE2ESandboxContainerRunningState(t, ctx, dockerClient, sandboxID, true)

	secondAddress := unusedLoopbackAddress(t)
	secondBaseURL := "http://" + secondAddress
	secondDaemon := startE2EDaemonWithEnv(t, binary, repoRoot, testRoot, secondAddress, image, map[string]string{
		"AGENT_COMPOSE_RUNTIME_BASE_URL": dockerDesktopRuntimeBaseURL(t, secondAddress),
	})
	waitForE2EDaemon(t, ctx, secondDaemon, secondBaseURL)
	secondHTTPClient := newE2EHTTPClient()
	secondRunClient := agentcomposev2connect.NewRunServiceClient(secondHTTPClient, secondBaseURL)

	var completed *agentcomposev2.RunDetail
	waitForE2ECondition(t, 30*time.Second, func() bool {
		response, getErr := secondRunClient.GetRun(ctx, connect.NewRequest(&agentcomposev2.GetRunRequest{RunId: runID}))
		if getErr != nil {
			return false
		}
		completed = response.Msg.GetRun()
		return completed.GetSummary().GetStatus() == agentcomposev2.RunStatus_RUN_STATUS_FAILED
	}, "interrupted run did not recover to failed")
	if summary := completed.GetSummary(); summary.GetExitCode() == 0 || !strings.Contains(summary.GetError(), "daemon interrupted") || summary.GetCompletedAt() == nil || completed.GetCleanupError() != "" {
		t.Fatalf("recovered run = %#v, want failed interrupted result without cleanup error", completed)
	}
	assertE2ESandboxContainerRunningState(t, ctx, dockerClient, sandboxID, false)
}

func applyE2ERunCompletionProject(t *testing.T, ctx context.Context, client agentcomposev2connect.ProjectServiceClient, root, image string) string {
	t.Helper()
	response, err := client.ApplyProject(ctx, connect.NewRequest(&agentcomposev2.ApplyProjectRequest{
		Spec: &agentcomposev2.ProjectSpec{
			Name: "docker-run-completion-e2e",
			Agents: []*agentcomposev2.AgentSpec{{
				Name: "worker", Provider: "codex", Image: image,
				Driver: &agentcomposev2.DriverSpec{Name: "docker", Config: &agentcomposev2.DriverSpec_Docker{Docker: &agentcomposev2.DockerDriverSpec{}}},
			}},
		},
		Source: &agentcomposev2.ProjectSource{ComposePath: filepath.Join(root, "agent-compose.yml"), ProjectDir: root},
	}))
	if err != nil {
		t.Fatalf("ApplyProject returned error: %v", err)
	}
	projectID := response.Msg.GetProject().GetSummary().GetProjectId()
	if !response.Msg.GetApplied() || projectID == "" {
		t.Fatalf("ApplyProject response = %#v, want applied project", response.Msg)
	}
	return projectID
}

func waitForE2ERunningRunSandbox(t *testing.T, ctx context.Context, runClient agentcomposev2connect.RunServiceClient, dockerClient *client.Client, runID string) string {
	t.Helper()
	var sandboxID string
	waitForE2ECondition(t, 2*time.Minute, func() bool {
		response, err := runClient.GetRun(ctx, connect.NewRequest(&agentcomposev2.GetRunRequest{RunId: runID}))
		if err != nil {
			return false
		}
		summary := response.Msg.GetRun().GetSummary()
		if summary.GetStatus() == agentcomposev2.RunStatus_RUN_STATUS_FAILED || summary.GetStatus() == agentcomposev2.RunStatus_RUN_STATUS_CANCELED || summary.GetStatus() == agentcomposev2.RunStatus_RUN_STATUS_SUCCEEDED {
			t.Fatalf("run reached unexpected terminal state before hard kill: status=%s error=%q", summary.GetStatus(), summary.GetError())
		}
		if summary.GetStatus() != agentcomposev2.RunStatus_RUN_STATUS_RUNNING {
			return false
		}
		sandboxID = summary.GetSandboxId()
		if sandboxID == "" {
			return false
		}
		containers, err := listE2EDockerSandboxContainers(ctx, dockerClient, sandboxID)
		return err == nil && len(containers) == 1 && containers[0].State == "running"
	}, "run did not start in a running Docker sandbox")
	return sandboxID
}

func assertE2ESandboxContainerRunningState(t *testing.T, ctx context.Context, dockerClient *client.Client, sandboxID string, wantRunning bool) {
	t.Helper()
	containers, err := listE2EDockerSandboxContainers(ctx, dockerClient, sandboxID)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range containers {
		if item.State == "running" {
			if !wantRunning {
				t.Fatalf("Docker sandbox container %s remains running after interrupted run recovery", item.ID)
			}
			return
		}
	}
	if wantRunning {
		t.Fatalf("Docker sandbox %s stopped before daemon restart recovery", sandboxID)
	}
}

func listE2EDockerSandboxContainers(ctx context.Context, dockerClient *client.Client, sandboxID string) ([]containerapi.Summary, error) {
	args := filters.NewArgs(
		filters.Arg("label", "agent-compose.sandbox_id="+sandboxID),
		filters.Arg("label", "agent-compose.driver=docker"),
	)
	return dockerClient.ContainerList(ctx, containerapi.ListOptions{All: true, Filters: args})
}
