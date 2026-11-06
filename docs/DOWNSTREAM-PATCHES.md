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
| D-002 | `.github/workflows/release.yml` | workflow override | Upstream's `sync-version-file` job writes the released version into `backend/cmd/server/VERSION`, a file the synchronisation copier also rewrites from upstream. A downstream release tag carries a `-custom.N` suffix that upstream never uses, so writing it there would put a value in a vendored file that the next sync overwrites. The job is limited to plain upstream-shaped tags and the downstream revision lives in `.github/custom-revision`. | Revisited when upstream changes the version write-back or the tag naming it expects. | maintainers |
| D-003 | `.github/workflows/ci.yml` | runner override | The `scripts` job needs macOS for its Bash 3.2 and BSD `stat` coverage, but hosted macOS runners are the scarcest pool GitHub offers. The job completes in well under a minute once a runner is assigned, so an unset timeout cancels it while it is still queued and reports a code failure for work that never ran. The job sets an explicit `timeout-minutes` sized for the queue. | Revisited if the fixtures stop depending on macOS-only tool behaviour or GitHub changes hosted macOS capacity. | maintainers |
| D-004 | `.github/scripts/version-bump.sh`, `.github/custom-revision` | added files | Downstream releases need a version that names both the upstream base and this distribution's own revision. The upstream part is read from the vendored `backend/cmd/server/VERSION`, which the sync refreshes, and the revision counter is kept outside `backend/` so a sync cannot reset it. | Additive; consumed only by `.github/workflows/upstream-sync.yml` and `.github/scripts/resolve-version.sh`. | maintainers |
| D-005 | `.github/scripts/sync-tags.sh`, `.github/workflows/upstream-sync.yml` | workflow constraint | The synchronisation pipeline needs two repository settings that a workflow file cannot declare. Mirroring an upstream release tag creates a tag whose target tree carries upstream's own `.github/workflows/` files, and GitHub rejects that from a workflow token unless the GitHub Actions app holds the `Workflows` permission; without it the mirror produces no tags and every tag build, image, and release is skipped. Opening the sync pull request requires **Allow GitHub Actions to create and approve pull requests**, without which `gh pr create` fails and an advance stops on the sync branch. The workflow sets `GH_REPO` because it adds the upstream repository as a remote and `gh` would otherwise address upstream, which is why the label lookup first reported the sync labels as missing. | Revisited when either operation starts failing again, or if upstream releases start pointing at trees without workflow files. | repository administrators |
| D-006 | `backend/internal/pkg/locale/manifest.json`, `.github/downstream-locales.json`, `.github/scripts/locale-merge.py` | added locale | The frontend and backend share one locale manifest, and it lives under a vendored directory the synchronisation copier rewrites from upstream, so a locale added directly to it disappears on the next sync. Listing the whole file in `preserveLocalPaths` would keep the locale but convert every upstream manifest edit into a manual merge. Instead the downstream locale entries are declared in `.github/downstream-locales.json` and re-applied to whatever upstream currently ships, by code whose output differs from upstream by one added line. The merge is idempotent, keeps upstream's one-locale-per-line layout, and treats an upstream locale with the same code as authoritative. | Additive and self-maintaining; revisited if upstream changes the manifest schema or starts shipping a `ru` locale. | maintainers |

## Conflict handling

When the nightly sync reports a conflict, the affected rows appear in
`sync-report.json`. Resolve by one of:

1. **Adopt upstream.** Remove the row and take the upstream version.
2. **Keep local.** Re-apply the local change after the sync commit and update
   the `Upstream interaction` column to describe how the patch is maintained.
3. **Reconcile.** Merge both, then document the merged behaviour in the row.

Every resolution lands in the same pull request as the sync commit.
