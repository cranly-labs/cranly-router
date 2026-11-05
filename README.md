<!-- Replace the single placeholder token cranly-labs in the links and badge URLs below with your GitHub organization or user name. Everything else can stay as written. -->

# cranly-router

**A hardened, reproducible, supply-chain-aware downstream distribution of [TokenRouter](https://github.com/TokenFlux/TokenRouter), deployed Coolify-first.**

[![CI](https://github.com/cranly-labs/cranly-router/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/cranly-labs/cranly-router/actions/workflows/ci.yml)
[![Upstream Sync](https://github.com/cranly-labs/cranly-router/actions/workflows/upstream-sync.yml/badge.svg?branch=main)](https://github.com/cranly-labs/cranly-router/actions/workflows/upstream-sync.yml)
[![Container](https://img.shields.io/badge/ghcr.io-cranly--router-3F3F46?logo=docker&logoColor=white)](https://github.com/cranly-labs/cranly-router/pkgs/container/cranly-router)
[![License](https://img.shields.io/badge/license-LGPL--3.0--or--later-3F3F46)](LICENSE)
[![Platforms](https://img.shields.io/badge/platforms-linux%2Famd64%20%7C%20linux%2Farm64-3F3F46)](https://github.com/cranly-labs/cranly-router/pkgs/container/cranly-router)

---

## Overview

`cranly-router` is an independently maintained downstream build of **TokenRouter**, the open-source LLM API gateway. It tracks the upstream project closely, keeps the application code unmodified wherever possible, and adds a thin, auditable layer of production-hardening and supply-chain controls around the build and deployment path.

This distribution exists for platform-engineering teams that need to:

- **Run TokenRouter from an image they can reason about.** The runtime image is rebuilt from a hardened Dockerfile with pinned and overridable base images, non-root execution, and explicit OCI metadata.
- **Verify what they deploy.** Every release image ships with a Software Bill of Materials (SBOM) and build provenance attestation, so artifacts can be traced back to a commit, a workflow run, and a signed build.
- **Deploy the same way every time.** A Coolify-first deployment artifact set provides a single, reviewable Compose file and documented environment contract for repeatable rollouts.
- **Stay current without manual merging.** A scheduled workflow pulls upstream changes into reviewable pull requests, so the distribution does not silently drift behind upstream security fixes.
- **Give AI agents a safe contract.** Explicit contribution rules define where local customisation is allowed and where upstream sources remain authoritative.

This project is a *distribution*, not a fork with application-level feature work. The gateway, its protocols, its frontend, and its plugin SDK are upstream code. The value added here is the hardened delivery surface around it.

> This distribution is an independent downstream build. It is not affiliated with, sponsored by, or endorsed by TokenFlux. See [License and attribution](#license-and-attribution).

---

## What this distribution changes

| Concern | Upstream TokenFlux/TokenRouter | cranly-router |
| --- | --- | --- |
| Runtime image | Multi-stage image built from the upstream Dockerfile | Hardened runtime image: non-root user, minimal runtime package set, explicit entrypoint and signal handling, read-only-friendly filesystem assumptions |
| Base images | Build args with upstream defaults | Pinned defaults, fully overridable via build args so operators can substitute verified bases under their own patch policy |
| OCI metadata | Baseline labels | Extended `org.opencontainers.image.*` labels (source, revision, version, created, licenses) for provenance and registry tooling |
| SBOM and provenance | Not part of the default image contract | SBOM generated per build and signed provenance attestation attached to pushed images |
| Image tag policy | `latest` and release tags | Documented tag policy: `latest`, `vX.Y.Z`, `sha-<short7>`, `edge`, with a clear meaning and stability guarantee for each |
| Deployment | Compose, install script, standalone container | Coolify-first Compose artifact under `deploy/coolify/` plus documented standalone options |
| Upstream tracking | n/a (is upstream) | Scheduled automated sync workflow opening reviewable pull requests with a label contract |
| Contributor contract | Upstream `AGENTS.md` and docs | Additional AI-agent contract in `docs/ai/CONTRIBUTING-AI.md` and root `AGENTS.md`, plus `docs/DOWNSTREAM-PATCHES.md` to record every deviation |

---

## Architecture

```text
+------------------------------+
|  Upstream repository         |
|  TokenFlux/TokenRouter@main  |
+--------------+---------------+
               |
               | scheduled sync workflow (fetch, rebase, open PR)
               v
+------------------------------+
|  main of this repository     |
|  cranly-router |
+--------------+---------------+
               |
               | CI build and test (lint, unit, integration, image build)
               v
+------------------------------+
|  Container image             |
|  ghcr.io/cranly-labs/        |
|    cranly-router             |
+--------------+---------------+
               |
               | pull by tag
               v
+------------------------------+
|  Coolify deployment          |
|  deploy/coolify/             |
+------------------------------+
```

Each stage is independently observable: sync produces a pull request, CI produces a status check and an attested image, and deployment consumes a specific image tag rather than a floating reference.

---

## Container images

Images are published to the GitHub Container Registry:

```text
ghcr.io/cranly-labs/cranly-router
```

| Tag | Meaning | Mutability | Recommended use |
| --- | --- | --- | --- |
| `latest` | Most recent successful build from `main` | Moves on every successful `main` build | Evaluation, staging |
| `vX.Y.Z` | Release aligned to an upstream version | Immutable once published | Production |
| `sha-<short7>` | Build for a specific commit (first 7 characters) | Immutable | Bisecting, rollback, forensics |
| `edge` | Bleeding-edge build from the sync branch or pre-release state | Moves frequently | Early testing only |

**Supported platforms:** `linux/amd64` and `linux/arm64`.

For production, pin an immutable tag:

```bash
docker pull ghcr.io/cranly-labs/cranly-router:v0.6.1
```

> Tag availability depends on whether the corresponding build and attestation steps have completed. Prefer immutable tags in any environment you depend on.

---

## Deployment on Coolify

The recommended production path for this distribution is [Coolify](https://coolify.io).

1. In Coolify, create a new resource of type **Docker Compose**.
2. Point it at this repository (or at the raw Compose file) using `deploy/coolify/docker-compose.yml`.
3. Set the environment variables described in `deploy/coolify/.env.example`. Generate independent secrets for the database and application; never reuse example values.
4. Set the image tag explicitly to a `vX.Y.Z` release tag instead of `latest`.
5. Deploy, then verify the health endpoint and confirm the database and cache services are reachable from the application container.
6. Configure persistent volumes for the database and any plugin or upload data, and back them up on your own schedule.

Full operator guidance, including required variables, volume layout, and upgrade steps, lives in [`deploy/coolify/README.md`](deploy/coolify/README.md).

<details>
<summary>Coolify checklist before going live</summary>

- Immutable image tag pinned, not `latest`.
- Database password and application secrets generated independently and stored in Coolify secrets.
- Persistent volumes mapped and included in backups.
- HTTPS terminated at Coolify with a valid certificate.
- Health checks enabled and alerting wired to your on-call channel.
- Rollback target image tag recorded.

</details>

---

## Deployment with Docker Compose

The Coolify Compose artifact is a normal Compose file and can be used directly:

```bash
docker compose -f deploy/coolify/docker-compose.yml up -d
```

Upstream also documents standalone and scripted options. When you deploy without Coolify, you are responsible for TLS termination, secret management, and host-level hardening. Treat the Coolify artifact as the reference configuration and adapt it deliberately rather than inventing a parallel stack.

---

## Staying in sync with upstream

This distribution is kept current by an automated synchronisation workflow rather than by manual merges.

**Model.** A scheduled GitHub Actions workflow (`.github/workflows/upstream-sync.yml`) fetches `TokenFlux/TokenRouter@main`, advances a reusable `sync/upstream-main` branch to the upstream snapshot while preserving every path listed in `.upstream-sync.json`, and opens or refreshes a pull request. The workflow never pushes to `main`; every synchronisation lands as a reviewable change set. A separate watcher (`.github/workflows/upstream-release-watch.yml`) polls the upstream release feed so a new tag shortens the delay beyond the nightly window.

**What a sync pull request looks like.** It contains only upstream commits plus, when required, minimal conflict-resolution commits confined to downstream-owned files. The PR body summarises the upstream range covered, the upstream version, and any files that required manual resolution.

**Label contract.** Sync pull requests carry labels used to drive dashboards and review routing:

| Label | Meaning |
| --- | --- |
| `upstream-sync` | Opened by the automated synchronisation workflow |
| `sync-conflict` | Upstream touched a path this distribution preserves; the guard workflow blocks merge until resolved |
| `sync-automerge` | Opt-in marker for a fast-track merge once every required check is green |

The label names are defined once in `.upstream-sync.json` and are used by the
sync, guard and watcher workflows, so they can be renamed in a single place.

**Resolving conflicts.** Keep local changes minimal and confined to files this distribution owns: `docker/`, `deploy/coolify/`, `.github/workflows/`, `docs/`, and the root documentation files. When a conflict touches upstream application code, prefer the upstream version and move the customisation into a downstream-owned file. Record any resulting behavioural deviation in `docs/DOWNSTREAM-PATCHES.md` in the same pull request.

Never force-push `main`, and never hand-edit generated sync metadata.

---

## Repository layout

| Path | Purpose |
| --- | --- |
| `backend/` | Upstream Go 1.27 gateway application |
| `frontend/` | Upstream Vue 3 + pnpm web application |
| `plugin-sdk/` | Upstream plugin SDKs (MIT), with `go/` and `web/` packages |
| `deploy/` | Deployment artifacts; `deploy/coolify/` holds the hardened Compose stack |
| `docker/` | Hardened Dockerfile and build support files owned by this distribution |
| `.github/workflows/` | CI, upstream synchronisation, and supply-chain workflows |
| `docs/` | Downstream documentation, including `docs/ai/` and `docs/DOWNSTREAM-PATCHES.md` |
| `tools/` | Upstream development and maintenance tooling |
| `assets/` | Upstream brand and documentation assets |

The application source trees mirror upstream. Downstream-owned paths are intentionally narrow so upstream merges stay mechanical.

---

## Contributing

Human contributors should start with [`CONTRIBUTING.md`](CONTRIBUTING.md) for environment setup, branch naming, commit conventions, and review expectations. CI must pass before a pull request is reviewable.

AI coding agents must read [`docs/ai/CONTRIBUTING-AI.md`](docs/ai/CONTRIBUTING-AI.md) and the root [`AGENTS.md`](AGENTS.md) before making changes. The short version: upstream sources are authoritative, local patches must stay minimal and documented, and every deviation from upstream is recorded in `docs/DOWNSTREAM-PATCHES.md`.

<details>
<summary>Where local customisation belongs</summary>

- `docker/` — hardened image build
- `deploy/coolify/` — Coolify-first deployment stack
- `.github/workflows/` — CI, sync, and attestation automation
- `docs/` and root documentation — contributor and operator guides

Anything outside these paths should match upstream.

</details>

---

## Security

Report vulnerabilities privately through **GitHub Security Advisories** on this repository. Do not open a public issue for a suspected vulnerability. Response expectations and scope are described in [`SECURITY.md`](SECURITY.md).

Hardening measures applied by this distribution:

- Hardened runtime image with a non-root process user and a minimal runtime package set.
- Pinned base images with a single, documented override point for each.
- SBOM generated for every published image and retained with the release.
- Build provenance attestation attached to pushed images.
- Explicit image tag policy so production can pin immutable references.
- Coolify-first deployment artifact with a documented environment contract.
- Automated upstream synchronisation so security fixes upstream are surfaced as reviewable pull requests.

---

## License and attribution

Upstream **TokenRouter** is licensed under the **GNU Lesser General Public License v3.0 or later** (LGPL-3.0-or-later). The upstream `plugin-sdk/` is licensed under the **MIT License**. Upstream copyright is:

```text
Copyright (c) 2026 Wesley Liddick & TokenFlux
```

This repository is an **independent downstream distribution**. It is **not affiliated with, sponsored by, or endorsed by TokenFlux**. It must not be operated, presented, or marketed in a way that impersonates the official TokenFlux service, and it must not reuse TokenFlux branding, logos, domain names, or promotional material in ways that falsely suggest official operation, authorisation, partnership, or endorsement.

This section extends the intent of the upstream `BRANDING_EN.md` commercial-use and branding notice. Operators deploying this distribution are responsible for choosing a distinct site name, visual identity, and service description, and for clearly identifying the actual operator and contact details. All copyright, license, and attribution notices required by the applicable licenses must be retained.

Truthful attribution such as "Built with TokenRouter" is permitted where it does not misleadingly suggest an official relationship. Nothing in this notice reduces the rights granted by the applicable open-source licenses.
