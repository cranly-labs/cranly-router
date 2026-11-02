# Security Policy

This policy covers `tokenrouter-hardened-mirror`, the hardened downstream distribution of upstream `TokenFlux/TokenRouter`. It describes supported versions, how to report a vulnerability privately, what reporters can expect, and the scope of this policy.

---

## Supported versions

| Version | Supported |
| --- | --- |
| Latest `vX.Y.Z` release | Yes |
| `latest` / `edge` images from `main` | Best effort; no guarantee of a stable security response window |
| Older `vX.Y.Z` releases | No; upgrade to the latest release |
| Local or modified builds | No; the policy applies to artifacts published from this repository |

Because this is a downstream distribution, support is bound to the upstream version it mirrors. When upstream stops supporting a version, this distribution stops supporting the corresponding release tag.

---

## Reporting a vulnerability

**Do not open a public issue for a suspected vulnerability.**

Report privately through GitHub Security Advisories:

1. Go to the **Security** tab of this repository.
2. Select **Report a vulnerability**.
3. Provide a clear description, affected version or image tag, impact, reproduction steps, and any suggested remediation.

Include, where applicable:

- the image tag or release version;
- whether the issue affects the hardened build and deployment artifacts, or upstream application code;
- a minimal reproduction or proof of concept;
- any known workaround.

Do not include real credentials, customer data, or production secrets in a report. Redact as needed.

If your report concerns upstream application behaviour, please report it upstream as well, following the upstream project's security process. Coordinated disclosure through both channels is appreciated.

---

## Response expectations

- **Acknowledgement:** initial acknowledgement as soon as practical after the report is received.
- **Triage:** an assessment of severity, affected versions, and whether the issue is a build or deployment artifact issue or an upstream application issue.
- **Remediation:** fixes are delivered through the normal release path as hardened images, once validated.
- **Disclosure:** coordinated with the reporter. Timelines depend on severity and on upstream responsiveness for upstream application defects.

This project is maintained on a best-effort basis and does not offer a paid support or SLA commitment. Reports that include a clear reproduction and impact assessment move faster.

---

## Hardening measures in this distribution

The following controls are applied to the build and deployment path:

- **Hardened runtime image.** The runtime stage runs as a non-root user with a minimal package set and an explicit entrypoint, and is designed to work with a read-only filesystem where the application does not require writes.
- **Pinned and overridable base images.** Base images are pinned by default and can be overridden through build arguments, allowing operators to substitute bases under their own patch policy.
- **OCI metadata.** Published images carry `org.opencontainers.image.*` labels for source, revision, version, creation time, and licences.
- **SBOM.** A Software Bill of Materials is generated for every published image and retained with the release.
- **Provenance attestation.** Build provenance is attached to pushed images so consumers can verify how and from which commit an image was produced.
- **Image tag policy.** Documented tag semantics with immutable `vX.Y.Z` and `sha-<short7>` tags for production use.
- **Coolify-first deployment artifact.** A single reviewed Compose stack with an explicit environment contract, rather than ad hoc deployment instructions.
- **Automated upstream synchronisation.** A scheduled workflow surfaces upstream security fixes as reviewable pull requests.

These measures reduce risk in the delivery path. They do not make the application itself secure, and they do not remove the operator's responsibility for secret management, network exposure, TLS, backups, and access control.

---

## Scope

### In scope

- The hardened Dockerfile and container image published from this repository.
- Build and release workflows in `.github/workflows/`, including SBOM and attestation steps.
- Coolify and Compose deployment artifacts under `deploy/coolify/`.
- Downstream documentation where it could plausibly lead an operator into an insecure configuration.

### Out of scope

- **Upstream application vulnerabilities.** Defects in the gateway application itself, its protocol handling, the web frontend, or the plugin SDK are upstream issues. Report them through the upstream project's security channel as well; this distribution cannot fix them independently of upstream.
- **Third-party dependencies.** Report to the relevant upstream dependency project, and upstream TokenRouter if it is affected.
- **Operator environment issues.** Misconfigured TLS, exposed databases, reused or weak secrets, absent backups, and host-level compromises are the operator's responsibility.
- **Denial of service from resource exhaustion** in configurations that skip documented limits.
- **Issues in unsupported or heavily modified local builds.**

If you are unsure which category an issue falls into, report it privately here and it will be routed appropriately.

---

## Operator responsibilities

Deployers of this distribution are expected to:

- pin immutable image tags in production;
- generate unique secrets for every deployment and store them in a secret manager;
- terminate TLS and restrict network exposure to the application and its datastores;
- apply volume backups and test restoration;
- monitor upstream security advisories for TokenRouter and for this distribution.

See [`README.md`](README.md) and [`deploy/coolify/README.md`](deploy/coolify/README.md) for deployment guidance.
