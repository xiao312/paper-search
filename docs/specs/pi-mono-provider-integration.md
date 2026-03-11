# pi-mono provider/model integration (phase 1)

## Goal

Leverage `pi` (from `badlogic/pi-mono`) as an LLM execution bridge so this project can use a wide set of providers/models without re-implementing provider SDKs.

## Why this approach

- Existing Python project stays lightweight.
- Provider/model support follows `pi` releases.
- Credentials are already handled by `pi` (`auth.json`, env vars, OAuth `/login`).

Reference docs inspected:
- `packages/coding-agent/docs/providers.md`
- `packages/coding-agent/docs/models.md`
- `packages/coding-agent/docs/sdk.md`

## Implemented surface

### CLI

- `papersearch llm list-models [--provider <name>] [--search <term>]`
- `papersearch llm prompt <prompt> [--provider <name>] [--model <id>] [--thinking <level>]`

### MCP tools

- `llm_list_models`
- `llm_prompt`

### Internal module

- `src/papersearch/integrations/pi_mono_client.py`
  - wraps subprocess calls to `pi`
  - returns structured result (`ok`, stdout/stderr, return code, command)

## Runtime requirements

- `pi` command available in PATH
- provider auth configured in pi (from `providers.md`):
  - env vars and/or `~/.pi/agent/auth.json`
  - optional OAuth login (`/login`) for subscription providers

## Notes

- This phase intentionally uses `pi` CLI subprocess bridge.
- Future upgrade path: direct Node SDK microservice sidecar using `@mariozechner/pi-coding-agent` if streaming/advanced control is needed.
