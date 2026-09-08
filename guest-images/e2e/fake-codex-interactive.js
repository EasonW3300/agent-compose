#!/usr/bin/env node

// This process emulates the tiny newline-delimited JSON surface used by the
// Codex SDK. A new SDK thread requests the missing path; its resumed thread
// confirms completion, so the Docker E2E verifies both conversation turns.
const isResume = process.argv.includes("resume");
const threadID = "agent-compose-e2e-thread";
const envelope = isResume
  ? { state: "completed", message: "已根据提供的路径完成整理。" }
  : { state: "needs_input", message: "请提供工作日志路径。" };

function emit(event) {
  process.stdout.write(`${JSON.stringify(event)}\n`);
}

// Wait for the SDK to finish writing its prompt before closing stdout. This
// avoids a racing broken pipe while keeping the fixture independent of prompt text.
process.stdin.resume();
process.stdin.once("end", () => {
  emit({ type: "thread.started", thread_id: threadID });
  emit({
    type: "item.completed",
    item: {
      id: isResume ? "completed-answer" : "question-answer",
      type: "agent_message",
      text: JSON.stringify({ agent_compose: envelope }),
    },
  });
  emit({
    type: "turn.completed",
    usage: { input_tokens: 0, cached_input_tokens: 0, output_tokens: 0, reasoning_output_tokens: 0, total_tokens: 0 },
  });
});
