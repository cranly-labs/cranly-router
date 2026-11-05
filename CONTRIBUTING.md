# Contributing

Thanks for contributing to `cranly-router`, a hardened downstream distribution of upstream [TokenFlux/TokenRouter](https://github.com/TokenFlux/TokenRouter).

This guide is for human contributors. Agent-authored work must also follow [`docs/ai/CONTRIBUTING-AI.md`](docs/ai/CONTRIBUTING-AI.md) and the root [`AGENTS.md`](AGENTS.md).

---

## What belongs here

This repository tracks upstream closely and adds a narrow layer of build, deployment, and supply-chain hardening. Good contributions here:

- improve the hardened image build under `docker/`;
- improve Coolify and Compose deployment under `deploy/coolify/`;
- improve CI, sync, SBOM, or attestation automation under `.github/workflows/`;
- improve documentation and the contributor contract.

Application behaviour, protocols, and UI are upstream concerns. Take those upstream rather than patching them here.

---

## Development environment

The project is a monorepo:

| Area | Technology | Notes |
| --- | --- | --- |
| `backend/` | Go 1.27 | Gateway application, embedded frontend assets |
| `frontend/` | Vue 3 + pnpm | Web application |
| `plugin-sdk/` | Go and TypeScript | Plugin SDKs, MIT licensed |
| `deploy/` | Docker Compose | Includes the Coolify-first stack |
| `docker/` | Docker | Hardened image build owned by this distribution |

Common commands from the repository root:

```bash
make fmt          # format Go, frontend, and plugin SDK sources
make fmt-check    # verify formatting
make lint         # lint all components
make test         # unit tests
make test-integration
make build
make verify       # closest local equivalent to full CI
make security     # security scans
```

Building the hardened image locally:

```bash
docker build -f docker/Dockerfile -t tokenrouter-hardened:local .
```

Validating the Coolify stack:

```bash
docker compose -f deploy/coolify/docker-compose.yml config
```

Toolchain versions are expected to match upstream runtime requirements. Check the repository's pinned version files before reporting environment problems.

---

## Branch naming

Use one of the following prefixes:

- `codex/` — branches produced by automated or agent-assisted work;
- `feat/` — human feature or improvement branches.

Keep names short, lowercase, and hyphenated, and describe the change:

```text
feat/coolify-health-checks
codex/harden-runtime-user
```

---

## Commits

- Use **Conventional Commits**: `type(scope): subject`.
- Write subjects and bodies in **English**.
- One concern per commit; avoid mixing unrelated changes.
- Keep the subject imperative and concise.
- Record any deviation from upstream in `docs/DOWNSTREAM-PATCHES.md` within the same change set.

Examples:

```text
fix(docker): pin alpine base tag
feat(deploy): add Coolify compose stack
ci(sync): open upstream sync pull requests on a schedule
docs(patches): document hardened entrypoint deviation
```

---

## Pull requests

Before opening a pull request:

1. Rebase or merge the latest `main` so the branch is current.
2. Run the checks covering your change, at minimum `make fmt-check`, `make lint`, and the relevant tests.
3. Run `make verify` for changes that touch the image build, workflows, or deployment artifacts.
4. Confirm the Compose file still validates if you changed deployment artifacts.
5. Fill in the pull request description with what changed, why, and how it was verified.

**CI must pass.** A pull request is not ready for review while required checks are failing, and reviewers will not merge a branch with unresolved failures. If a check cannot run in your environment, state that explicitly and explain why.

### Review expectations

- Reviews focus on correctness, minimal footprint against upstream, and whether the deployment contract stays accurate.
- Changes confined to downstream-owned paths review quickly. Changes that touch upstream application files require an explicit justification and a deviation record.
- Expect reviewers to ask for a smaller diff. Prefer configuration over code and downstream files over upstream files.
- Pull requests that change documented deployment behaviour without updating the docs.
- Keep discussion on the pull request itself so decisions stay discoverable.

### What gets rejected

- Rewrites, reformatting, or mass renames of upstream files.
- Undocumented deviations from upstream.
- Rebranding of upstream modules, protocols, endpoints, environment variables, or SDK symbols.
- Committed secrets, real credentials, or production data.
- Force-pushes to `main` or edits to generated sync metadata.

---

## Reporting issues

- **Security issues:** do not open a public issue. Follow [`SECURITY.md`](SECURITY.md).
- **Upstream application bugs:** report them upstream to TokenFlux unless they specifically affect this distribution's build or deployment artifacts.
- **Distribution issues:** open an issue here with the image tag, the deployment target, and reproduction steps.

---

## Agent-authored contributions

Work produced or assisted by AI agents must follow [`docs/ai/CONTRIBUTING-AI.md`](docs/ai/CONTRIBUTING-AI.md). Openers should disclose in the pull request that the change was AI-generated or AI-assisted, and confirm that the recorded checks were actually run.
