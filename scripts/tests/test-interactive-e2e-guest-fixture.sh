#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
dockerfile="$ROOT_DIR/guest-images/Dockerfile.agent-compose-guest-e2e-interactive"
fake_codex="$ROOT_DIR/guest-images/e2e/fake-codex-interactive.js"

fail() {
  printf 'test-interactive-e2e-guest-fixture: %s\n' "$1" >&2
  exit 1
}

[[ -f "$dockerfile" ]] || fail "missing test-only guest Dockerfile"
[[ -f "$fake_codex" ]] || fail "missing deterministic Codex fixture"

grep -Fqx 'ARG BASE_IMAGE=local/agent-compose-guest:interactive' "$dockerfile" \
  || fail "test guest must default to the local interactive guest image"
grep -Fqx 'FROM ${BASE_IMAGE}' "$dockerfile" \
  || fail "test guest must derive from its explicit base image"
grep -Fqx 'ENV CODEX_BIN=/usr/local/bin/agent-compose-e2e-fake-codex' "$dockerfile" \
  || fail "test guest must override Codex only through its image environment"
grep -Fq 'COPY guest-images/e2e/fake-codex-interactive.js /usr/local/bin/agent-compose-e2e-fake-codex' "$dockerfile" \
  || fail "test guest must install the deterministic Codex fixture"
grep -Fq 'COPY runtime/javascript/dist /usr/lib/node_modules/@chaitin-ai/agent-compose-runtime/dist' "$dockerfile" \
  || fail "test guest must overlay the current-branch compiled runtime rather than inherit a stale runtime from its base image"
grep -Fq 'state: "needs_input"' "$fake_codex" \
  || fail "fixture must emit an explicit needs_input envelope"
grep -Fq 'state: "completed"' "$fake_codex" \
  || fail "fixture must emit an explicit completed envelope"
grep -Fq 'process.argv.includes("resume")' "$fake_codex" \
  || fail "fixture must complete only after the SDK resumes its conversation"
if rg -q 'Dockerfile\.agent-compose-guest-e2e-interactive|agent-compose-e2e-fake-codex' \
  "$ROOT_DIR/Taskfile.yml" "$ROOT_DIR/scripts/build-agent-compose-guest.sh" "$ROOT_DIR/guest-images/Dockerfile.agent-compose-guest"; then
  fail "production guest build defaults must not reference the E2E fixture"
fi

assert_turn_state() {
  local output=$1
  local expected=$2
  printf '%s\n' "$output" | node -e '
    const expected = process.argv[1];
    let actual = "";
    for (const line of require("fs").readFileSync(0, "utf8").trim().split("\n")) {
      const event = JSON.parse(line);
      if (event.type === "item.completed" && event.item?.type === "agent_message") {
        actual = JSON.parse(event.item.text).agent_compose?.state || "";
      }
    }
    process.exit(actual === expected ? 0 : 1);
  ' "$expected" || fail "fixture turn state = unexpected value, want $expected"
}

first_turn=$(printf 'prompt' | node "$fake_codex" exec --experimental-json)
assert_turn_state "$first_turn" needs_input
second_turn=$(printf 'reply' | node "$fake_codex" exec --experimental-json resume agent-compose-e2e-thread)
assert_turn_state "$second_turn" completed

printf 'test-interactive-e2e-guest-fixture: all checks passed\n'
