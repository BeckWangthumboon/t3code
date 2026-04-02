# OpenCode Provider Integration

## Status: V1 (basic functionality)

## Architecture

OpenCode is integrated as a third provider (`"opencode"`) alongside Codex and Claude Code. The implementation uses the `@opencode-ai/sdk` package (`createOpencode()`) which starts an OpenCode server process and provides a client for session management and prompting.

### Key Files

| Area | File |
|---|---|
| Adapter (runtime) | `apps/server/src/provider/Layers/OpencodeAdapter.ts` |
| Provider (install/version detection) | `apps/server/src/provider/Layers/OpencodeProvider.ts` |
| Service tags | `apps/server/src/provider/Services/OpencodeAdapter.ts`, `OpencodeProvider.ts` |
| Contracts | `packages/contracts/src/orchestration.ts`, `model.ts`, `settings.ts` |

## What Works

- **Provider detection**: Checks `opencode --version` to determine if OpenCode CLI is installed and available.
- **Model picker**: 6 hardcoded OpenCode Go models appear in the provider model picker:
  - `opencode-go/glm-5` (GLM-5)
  - `opencode-go/kimi-k2.5` (Kimi K2.5) -- default
  - `opencode-go/mimo-v2-pro` (MiMo-V2-Pro)
  - `opencode-go/mimo-v2-omni` (MiMo-V2-Omni)
  - `opencode-go/minimax-m2.7` (MiniMax M2.7)
  - `opencode-go/minimax-m2.5` (MiniMax M2.5) -- default for git text generation
- **Session lifecycle**: Start, stop, and list sessions.
- **Prompting (batch)**: Sends a prompt via `client.session.prompt()`, waits for the full response, then emits it as a single `content.delta` batch. No token-by-token streaming.
- **Text rendering**: Only `type: "text"` parts are included in the assistant response. Reasoning/thinking parts (`type: "reasoning"`) are filtered out.
- **Tool call display**: Tool invocations (`type: "tool"`) emit `item.started`/`item.completed` events so they show in the UI.
- **Interrupt**: Calls `client.session.abort()` to cancel in-progress turns.
- **Model switching**: `sessionModelSwitch: "in-session"` -- model can be changed between turns without restarting the session.
- **Permissions**: All permissions set to `allow` (edit, bash, webfetch) so no approval prompts interrupt the agent.
- **Session persistence**: Provider session directory recognizes `"opencode"` for thread-to-session bindings.

## What Doesn't Work / Limitations

### No streaming
The entire prompt response is collected and emitted as one batch `content.delta`. The UI will show no output until the full response is ready. This can feel slow for long-running turns.

The SDK supports SSE event streaming via `client.session.prompt()` with streaming options, but the v1 adapter does not use it.

### No thread history (`readThread`)
`readThread` returns an empty turns array. The SDK has session message history but the adapter does not map it. Reconnecting to an existing OpenCode session will not show previous messages.

### Rollback is a stub (`rollbackThread`)
`rollbackThread` calls `client.session.revert()` with an empty `messageID` and returns empty turns. Rollback does not reliably undo turns.

### Reasoning/thinking not rendered
Reasoning parts from the SDK (`type: "reasoning"`) are silently dropped. They are not shown in the UI at all, unlike Claude's thinking toggle which renders them in a collapsible section.

### No dynamic model discovery
Models are hardcoded in `OpencodeProvider.ts`. If OpenCode adds or removes models, the list must be updated manually. The SDK exposes `client.provider.list()` which returns all available models -- this could replace the hardcoded list in a future iteration.

### No approval/user-input flow
`respondToRequest` and `respondToUserInput` both return errors. Since permissions are set to `allow`, these should never be called in normal operation. If OpenCode ever needs user approval despite the config, the turn will fail.

### Tool call details are minimal
Tool calls emit `item.started`/`item.completed` with `itemType: "dynamic_tool_call"` and `title` set to `part.tool`. Tool input/output content is not included.

### No session resume
Each `startSession` call creates a new OpenCode server+client pair (`createOpencode()`). Previous OpenCode sessions are not resumed. If the T3 server restarts, existing OpenCode sessions are lost.

## SDK Notes

The adapter uses `createOpencode()` from `@opencode-ai/sdk` which:
1. Starts an OpenCode server process (Node child process)
2. Returns `{ client, server }` where `client` is an HTTP client
3. `server.close()` must be called to clean up the process

Each T3 session maps to one `createOpencode()` call, meaning one OpenCode server process per active thread. This is heavier than Codex (stdio) or Claude (subprocess) and could be a resource concern with many concurrent sessions.

## Implications for the codebase

- **ProviderKind is a 3-way union**: `"codex" | "claudeAgent" | "opencode"`. Any code that hardcoded the binary codex/claude split needed updating. Several places were missed in the initial pass and required follow-up fixes (ProviderSessionDirectory, ChatView modelOptionsByProvider, etc.).
- **Model capabilities are empty**: OpenCode models have no effort levels, no fast mode, no thinking toggle, no context window options. The `TraitsPicker` and `composerProviderRegistry` handle this by returning null/empty.
- **Text generation routing**: OpenCode is included in the `TextGenerationProvider` union but routes through the Codex text generation path. This may not work correctly for OpenCode's model providers.
