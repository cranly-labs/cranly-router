#!/usr/bin/env bash
# =============================================================================
# sync-upstream.sh - fast-forward a mirror branch to the upstream default branch
# =============================================================================
# Purpose
#   Fetches the configured upstream repository and rewrites the local checkout of
#   the mirror branch so that vendored upstream sources match upstream exactly,
#   while every path listed under sync.preserveLocalPaths in .upstream-sync.json
#   keeps the downstream version.
#
# Contract
#   - Exits 0 when the branch already matched upstream (no commit created).
#   - Exits 0 and writes a report when the branch was advanced.
#   - Exits 1 when a preserved path was also modified upstream; the caller is
#     expected to open a conflict pull request instead of merging automatically.
#
# Environment
#   MIRROR_ROOT        repository root (default: git toplevel)
#   UPSTREAM_REMOTE    git remote name to fetch (default: upstream)
#   UPSTREAM_BRANCH    upstream branch to track (default: main)
#   MIRROR_BRANCH      local branch to advance (default: current branch)
#   SYNC_REPORT        path of the generated JSON report
# =============================================================================

set -euo pipefail

MIRROR_ROOT="${MIRROR_ROOT:-$(git rev-parse --show-toplevel)}"
cd "$MIRROR_ROOT"

UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-upstream}"
UPSTREAM_BRANCH="${UPSTREAM_BRANCH:-main}"
MIRROR_BRANCH="${MIRROR_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"
SYNC_REPORT="${SYNC_REPORT:-${MIRROR_ROOT}/sync-report.json}"
METADATA_FILE="${MIRROR_ROOT}/.upstream-sync.json"

if [ ! -f "$METADATA_FILE" ]; then
  echo "sync-upstream: missing ${METADATA_FILE}" >&2
  exit 1
fi

mapfile -t PRESERVED_PATHS < <(python3 - "$METADATA_FILE" <<'PY'
import json, sys
meta = json.load(open(sys.argv[1]))
for path in meta["sync"]["preserveLocalPaths"]:
    print(path)
PY
)

UPSTREAM_URL="$(python3 - "$METADATA_FILE" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["upstream"]["gitUrl"])
PY
)"

# A shallow fetch keeps nightly runs cheap; the release sync passes a full fetch.
if ! git remote get-url "$UPSTREAM_REMOTE" >/dev/null 2>&1; then
  git remote add "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
fi

git fetch --no-tags --prune "$UPSTREAM_REMOTE" "$UPSTREAM_BRANCH"
UPSTREAM_REF="${UPSTREAM_REMOTE}/${UPSTREAM_BRANCH}"
UPSTREAM_SHA="$(git rev-parse "$UPSTREAM_REF")"
BASE_SHA="$(git merge-base HEAD "$UPSTREAM_REF" || true)"

if [ "$UPSTREAM_SHA" = "$(git rev-parse HEAD)" ]; then
  echo "sync-upstream: already at upstream ${UPSTREAM_SHA:0:12}"
  printf '{"status":"up-to-date","upstreamSha":"%s"}\n' "$UPSTREAM_SHA" >"$SYNC_REPORT"
  exit 0
fi

# Any preserved path that upstream touched since the merge base forces the
# conflict path, because the downstream copy would otherwise be overwritten.
CONFLICTS=()
if [ -n "$BASE_SHA" ]; then
  while IFS= read -r changed; do
    [ -z "$changed" ] && continue
    for kept in "${PRESERVED_PATHS[@]}"; do
      case "$changed" in
        "$kept"*)
          CONFLICTS+=("$changed")
          break
          ;;
      esac
    done
  done < <(git diff --name-only "$BASE_SHA" "$UPSTREAM_REF")
fi

# Overwrite vendored upstream files with the upstream revisions. Paths outside
# the preserved set are removed when upstream deleted them, so the tree converges
# on upstream plus the downstream layer.
SAFE_DIRECTORY="$(git rev-parse --git-dir)/info"
mkdir -p "$SAFE_DIRECTORY"

while IFS= read -r path; do
  case "$path" in
    *.github/*) continue ;;
  esac
  case "$path" in
    *upstream-sync.json*) continue ;;
  esac
  skip=0
  for kept in "${PRESERVED_PATHS[@]}"; do
    case "$path" in
      "$kept"*) skip=1; break ;;
    esac
  done
  [ "$skip" = 1 ] && continue
  git checkout "$UPSTREAM_REF" -- "$path"
done < <(git diff --name-only --no-renames HEAD "$UPSTREAM_REF")

# Apply deletions that upstream performed on paths that are not preserved.
while IFS= read -r path; do
  [ -z "$path" ] && continue
  case "$path" in
    *.github/*) continue ;;
  esac
  skip=0
  for kept in "${PRESERVED_PATHS[@]}"; do
    case "$path" in
      "$kept"*) skip=1; break ;;
    esac
  done
  [ "$skip" = 1 ] && continue
  if [ ! -e "$path" ] && [ -e "$path" ]; then rm -f "$path"; fi
done < <(git diff --name-only --diff-filter=D --no-renames HEAD "$UPSTREAM_REF")

if git diff --quiet && git diff --cached --quiet; then
  echo "sync-upstream: no textual change after applying upstream; recording revision"
fi

SHORT_SHA="${UPSTREAM_SHA:0:12}"
SUCCESS=1
if [ "${#CONFLICTS[@]}" -gt 0 ]; then
  SUCCESS=0
fi

python3 - "$SYNC_REPORT" "$UPSTREAM_SHA" "$SUCCESS" "${CONFLICTS[@]+"${CONFLICTS[@]}"}" <<'PY'
import json, sys
report, sha, ok, *conflicts = sys.argv[1:]
json.dump(
    {
        "status": "advanced" if ok == "1" else "conflict",
        "upstreamSha": sha,
        "conflicts": conflicts,
    },
    open(report, "w"),
    indent=2,
)
print()
PY

if [ "$SUCCESS" = 1 ]; then
  echo "sync-upstream: advanced to upstream ${SHORT_SHA}"
  exit 0
fi

echo "sync-upstream: upstream modified ${#CONFLICTS[@]} preserved path(s); manual review required" >&2
printf '%s\n' "${CONFLICTS[@]}" >&2
exit 1
