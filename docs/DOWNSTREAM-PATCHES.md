# Downstream Patch Register

This register records every intentional deviation from upstream
[TokenFlux/TokenRouter](https://github.com/TokenFlux/TokenRouter). It exists so
that the automated sync pipeline stays reviewable: when upstream touches a path
listed here, the sync job stops and asks for a human instead of silently
overwriting local intent.

## How to use this register

Add one row per deviation. Keep entries short and factual. A deviation is any
change in a vendored upstream path, or any new file whose behaviour depends on
upstream internals. Purely additive files under downstream-owned paths
(`docker/`, `deploy/coolify/`, `.github/`, `docs/`) do not require an entry
unless they constrain upstream code.

| ID | Path | Kind | Reason | Upstream interaction | Owner |
| --- | --- | --- | --- | --- | --- |
| D-000 | _(baseline)_ | none | The initial import tracks upstream `main` without local modification to vendored sources. | None | maintainers |
| D-001 | `docker/Dockerfile.hardened` | build override | Upstream's Dockerfile defaults `GOPROXY` to `goproxy.cn`, which resets HTTP/2 streams and truncates multi-megabyte module archives, aborting `go mod download` on GitHub-hosted runners. The hardened build defaults to the global Google endpoints and retries the download three times against a persistent cache mount. | Upstream default revisited whenever the upstream Dockerfile changes its proxy arguments; the override is confined to `GOPROXY`/`GOSUMDB` and the download step. | maintainers |

## Conflict handling

When the nightly sync reports a conflict, the affected rows appear in
`sync-report.json`. Resolve by one of:

1. **Adopt upstream.** Remove the row and take the upstream version.
2. **Keep local.** Re-apply the local change after the sync commit and update
   the `Upstream interaction` column to describe how the patch is maintained.
3. **Reconcile.** Merge both, then document the merged behaviour in the row.

Every resolution lands in the same pull request as the sync commit.
