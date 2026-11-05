# Contributing as an AI Agent

This document is the detailed instruction set for AI coding agents working in `cranly-router`. It supplements, and does not replace, the repository's contributor guides for humans.

Read this file together with [`AGENTS.md`](../../AGENTS.md) before making any change.

---

## Purpose of this repository

`cranly-router` is a hardened, production-focused **downstream distribution** of TokenFlux's upstream LLM API gateway, [TokenFlux/TokenRouter](https://github.com/TokenFlux/TokenRouter).

The repository contains upstream application sources — a Go 1.27 backend, a Vue 3 + pnpm frontend, and Go/web plugin SDKs — plus a thin slice of downstream-owned configuration that adds hardened image builds, Coolify-first deployment artifacts, supply-chain attestation, and automated upstream synchronisation.

The goal of this distribution is a reproducible, verifiable build and deployment path around unmodified application code. It is not a feature fork. Default to changing as little as possible.

---

## Relationship to upstream

Upstream is the source of truth for the application.

- Upstream code lives under `backend/`, `frontend/`, `plugin-sdk/`, `tools/`, and `assets/`.
- Upstream review and style rules remain authoritative for those paths.
- Upstream versioning, release tags, and licences apply to upstream code.
- Downstream changes are limited to the delivery surface described below.

When upstream and this repository disagree about application behaviour, upstream wins unless a documented downstream patch says otherwise.

---

## The golden rule

**Upstream sources are authoritative. Local patches must stay minimal, isolated, and documented.**

In practice:

1. **Minimal.** Change the smallest possible surface. If a change can be expressed as configuration rather than code, prefer configuration. If it can be expressed in a downstream-owned file rather than an upstream file, prefer the downstream file.
2. **Isolated.** Confine edits to the downstream-owned paths listed below. Do not spread a single concern across upstream application files.
3. **Documented.** Every deviation from upstream behaviour, file layout, or build steps is recorded in [`docs/DOWNSTREAM-PATCHES.md`](../DOWNSTREAM-PATCHES.md) in the same change set.

If you cannot satisfy all three, stop and reconsider the approach rather than proceeding.

---

## Where local customisation lives

Only these paths are owned by this distribution. Everything else should match upstream.

| Path | Purpose |
| --- | --- |
| `docker/` | Hardened Dockerfile and image build support |
| `deploy/coolify/` | Coolify-first Compose stack and environment contract |
| `.github/workflows/` | CI, upstream synchronisation, SBOM and attestation automation |
| `docs/` | Downstream documentation, including `docs/ai/` and `docs/DOWNSTREAM-PATCHES.md` |
| Root documentation | `README.md`, `AGENTS.md`, `CONTRIBUTING.md`, `SECURITY.md`, and related root-level files |

Rules for edits outside these paths:

- Treat the file as upstream. Do not restructure, re-format, rename, or partially rewrite it.
- Do not introduce downstream comments, markers, or authorship blocks into upstream files.
- If a genuine upstream change is required, justify it explicitly, keep it as small as a single hunk where possible, and record it in `docs/DOWNSTREAM-PATCHES.md`.

---

## Running the checks that matter

Run the checks below before committing. They are the ones CI enforces and the ones that catch the failure modes most likely to be caused by a downstream change: a broken image build, an out-of-date workflow, or an undocumented patch.

**Formatting and lint, repository-wide:**

```bash
make fmt
make fmt-check
make lint
```

**Tests:**

```bash
make test
make test-integration
```

**Build verification:**

```bash
make build
make verify
```

**Security scans:**

```bash
make security
```

**Container image build, using the hardened Dockerfile:**

```bash
docker build -f docker/Dockerfile.hardened -t cranly-router:local .
```

**Compose validation for the Coolify stack:**

```bash
docker compose -f deploy/coolify/docker-compose.yml config
```

Notes:

- `make verify` is the closest local equivalent to full CI. Run it for changes that touch the image build, workflows, or deployment artifacts.
- If a change affects only documentation, run at least the formatting and lint checks plus the Compose validation when the docs describe deployment.
- If any check cannot be run in the current environment, say so explicitly in your summary and mark the affected area as unverified.

---

## Handling a sync pull request conflict

Automated synchronisation opens pull requests that replay upstream commits. Conflicts are expected occasionally and must be resolved conservatively.

1. **Read the pull request body first.** It records the upstream commit range, the upstream version, and any files already resolved.
2. **Prefer upstream on upstream files.** If a conflict touches `backend/`, `frontend/`, `plugin-sdk/`, `tools/`, or `assets/`, take the upstream side and move any required customisation into a downstream-owned file instead.
3. **Resolve only in downstream-owned paths.** Conflicts inside `docker/`, `deploy/coolify/`, or `.github/workflows/` are resolved by hand, keeping the hardened behaviour intact.
4. **Keep the resolution in one commit.** Use a clear Conventional Commit subject, for example `chore(sync): resolve upstream conflict in deploy/coolify`.
5. **Record every behavioural deviation** produced by the resolution in `docs/DOWNSTREAM-PATCHES.md`.
6. **Re-run the checks** listed above, at minimum the ones covering the changed area, then the container build if the image is affected.
7. **Never hand-edit generated sync metadata.** Do not touch files the workflow produces or consumes to track sync state; if the metadata looks wrong, report it rather than editing it.
8. **Never force-push `main`.**

If a resolution would require broad changes to upstream code, stop and escalate in the pull request instead of rewriting upstream modules.

---

## Commit conventions

- Use **Conventional Commits**: `type(scope): subject`.
- Write the subject and body in **English**.
- Keep the subject line short and imperative.
- Use a scope that names the affected downstream area, for example `docker`, `deploy`, `ci`, `sync`, or `docs`.
- Reference the upstream version or commit range when the change follows a sync.

Examples:

```text
fix(docker): pin alpine base and enable non-root runtime user
feat(deploy): add Coolify compose stack with health checks
chore(ci): attach SBOM and provenance to pushed images
docs(patches): record hardened entrypoint deviation for v0.6.1
chore(sync): merge upstream v0.6.1 into main
```

Do not commit generated artifacts, local build output, or untracked scratch files.

---

## Recording deviations from upstream

Every deviation from upstream must be recorded in `docs/DOWNSTREAM-PATCHES.md`.

A deviation record should state:

- what was changed and in which files;
- why upstream behaviour or layout could not be used as-is;
- the impact on operators and on the deployment contract;
- how the deviation is expected to be reconciled the next time upstream changes the affected area.

Update the record in the same commit as the change. A deviation without a record is treated as a defect, not an accepted patch.

---

## Do not

- **Do not rewrite whole upstream files.** No reformatting, reordering, or mass renaming of upstream sources.
- **Do not force-push `main`.** Ever. History on the default branch is shared.
- **Do not hand-edit generated sync metadata.** Sync state is produced and consumed by the workflow.
- **Do not commit secrets.** No tokens, passwords, private keys, or real credentials in any file, including examples and test fixtures. Use placeholders.
- **Do not rename upstream identifiers to rebrand core code.** Do not rename modules, packages, protocols, endpoints, environment variables, or SDK symbols to reflect this distribution's name. Upstream names are part of the application's interface.
- **Do not add downstream markers to upstream files** such as banners, comments, or authorship notes.
- **Do not change deployment defaults silently.** Any change to ports, volumes, environment variables, or health checks belongs in the deployment docs and in `docs/DOWNSTREAM-PATCHES.md`.
- **Do not mix unrelated changes.** One concern per commit and, where practical, one concern per pull request.

---

## Working with this repository's documentation

- Root `README.md` is written for operators and evaluators. Keep it accurate when the build or deploy contract changes.
- `docs/ai/CONTRIBUTING-AI.md` (this file) is the detailed agent contract.
- `AGENTS.md` is the concise operational summary agents load first.
- `CONTRIBUTING.md` is for human contributors.
- `SECURITY.md` covers vulnerability reporting and hardening scope.

When a change alters documented behaviour, update the affected document in the same change set.
