# Use the bridge from a terminal or IDE

The bridge exposes a minimal OpenAI-compatible endpoint:
`POST /v1/chat/completions` at `http://<tailscale-ip>:8081/v1`.
Tools execute **on the bridge host**; the caller receives the final text. For live
tokens use the native chat-job SSE stream or the terminal client.

## OpenCode CLI (MIT)
Create `~/.config/opencode/opencode.json`:
```json
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "mylocal": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "MY local AI bridge",
      "options": {
        "baseURL": "http://<tailscale-ip>:8081/v1",
        "apiKey": "placeholder"
      },
      "models": {
        "agent": { "name": "Agent Bridge" }
      }
    }
  },
  "model": "mylocal/agent"
}
```
Then run `opencode` in any project. If `BRIDGE_API_TOKEN` is set, add the header via
OpenCode's custom provider headers if supported, or leave the bridge token unset.

## Codex CLI (Apache-2.0)
In `~/.codex/config.toml`:
```toml
model_provider = "mylocal"
model = "agent"

[model_providers.mylocal]
name = "MY local AI bridge"
base_url = "http://<tailscale-ip>:8081/v1"
env_key = "BRIDGE_API_KEY_PLACEHOLDER"
```
Codex expects tool calls from the model; this bridge returns final text only, so use
it for chat/explanations. For full tool use, use the Android app or `cli/agent_cli.py`.

## VS Code / other IDEs
Any extension that accepts a custom OpenAI-compatible base URL can use
`http://<tailscale-ip>:8081/v1` with model name `agent`. Set a dummy API key if the
extension requires one. For full tool access, drive the bridge with the terminal
client or the Android app.

## Terminal client (recommended)
From the Android repository:
```bash
cp .env.example .env     # set REMOTE_API_BASE_URL
python3 cli/agent_cli.py
```
Type prompts; use `/plan`, `/review`, `/files`, `/read`, `/run`, `/models`.
