# Client laboratory (T6)

These pinned CLI coding agents connect to the in-process TokenRouter gateway in `backend/internal/app/client_lab_scenario_test.go`. The suite exercises real client request builders, SSE decoders, and tool execution loops against scripted fake upstreams or live provider proxies without external databases.

Pinned packages in `package.json`:

- `@openai/codex` `0.171.0` (OpenAI Responses client)
- `@anthropic-ai/claude-code` `2.1.330` (Anthropic Messages client, tested in `--bare` and default modes)
- `@earendil-works/pi-coding-agent` `1.6.0` (`openai-completions`, `openai-responses`, `anthropic-messages`, and `google-generative-ai` modes)

Configuration templates live under `templates/codex/`, `templates/claude-code/`, and `templates/pi/`. Each test renders them into `t.TempDir()` with isolated home, config, and working directories.

## Scripted mode

Install the locked client binaries once, then run the T6 scenarios from the repository root:

```sh
cd tools/clientlab && npm ci
TOKENROUTER_CLIENTLAB=1 go -C backend test ./internal/app -run '^TestClientLab' -count=1 -timeout 30m -v
```

Ordinary `go test ./...` runs skip subprocess execution when `TOKENROUTER_CLIENTLAB` is unset, while still running template, output-parser, route-matrix, and safety-gate unit tests.

## Record client corpus

Set `TOKENROUTER_CLIENTLAB_MODE=record` to write raw client request captures into `backend/internal/protocol/testkit/testdata/corpus/_incoming/`, then sanitize and verify them with `cmd/corpus`:

```sh
TOKENROUTER_CLIENTLAB=1 TOKENROUTER_CLIENTLAB_MODE=record go -C backend test ./internal/app -run '^TestClientLab' -count=1 -v
go -C backend run ./internal/protocol/testkit/cmd/corpus import ./internal/protocol/testkit/testdata/corpus/_incoming
go -C backend run ./internal/protocol/testkit/cmd/corpus verify ./internal/protocol/testkit/testdata/corpus
```

`_incoming/` is ignored by Git so unsanitized captures stay out of `git status` and pre-push checks.

## Live canary container

Live mode (`TOKENROUTER_CLIENTLAB_MODE=live`) forwards requests through `mockupstream`'s `proxy-record` mode to real providers. Because a real model chooses the shell commands executed by each CLI agent, the test refuses to start unless `TOKENROUTER_CLIENTLAB_CONTAINER=1` is present. Only `Dockerfile` sets that variable in its entrypoint.

Build and run a disposable container when running live canaries:

```sh
docker build -f tools/clientlab/Dockerfile -t tokenrouter-clientlab .
docker run --rm \
  -e TOKENROUTER_CLIENTLAB_MODE=live \
  -e TOKENROUTER_CLIENTLAB_LIVE_OPENAI_BASE_URL \
  -e TOKENROUTER_CLIENTLAB_LIVE_OPENAI_API_KEY \
  -e TOKENROUTER_CLIENTLAB_LIVE_ANTHROPIC_BASE_URL \
  -e TOKENROUTER_CLIENTLAB_LIVE_ANTHROPIC_API_KEY \
  -e TOKENROUTER_CLIENTLAB_LIVE_GEMINI_BASE_URL \
  -e TOKENROUTER_CLIENTLAB_LIVE_GEMINI_API_KEY \
  -e TOKENROUTER_CLIENTLAB_LIVE_GROK_BASE_URL \
  -e TOKENROUTER_CLIENTLAB_LIVE_GROK_API_KEY \
  -e TOKENROUTER_CLIENTLAB_LIVE_DEEPSEEK_BASE_URL \
  -e TOKENROUTER_CLIENTLAB_LIVE_DEEPSEEK_API_KEY \
  tokenrouter-clientlab
```

Each family also accepts an optional `TOKENROUTER_CLIENTLAB_LIVE_<PROVIDER>_MODEL` (`OPENAI`, `ANTHROPIC`, `GEMINI`, `GROK`, `DEEPSEEK`) override when testing a specific upstream model ID. Upstream API keys stay on the proxy side and are not passed to the CLI agent subprocesses, which always authenticate to the local gateway with a synthetic lab key.
