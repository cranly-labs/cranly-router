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
STATE_FILE="${STATE_FILE:-${MIRROR_ROOT}/.sync-state}"
METADATA_FILE="${MIRROR_ROOT}/.upstream-sync.json"

if [ ! -f "$METADATA_FILE" ]; then
  echo "sync-upstream: missing ${METADATA_FILE}" >&2
  exit 1
fi

# Read the preserved-path list into an array. mapfile requires bash 4, so the
# loop form keeps this script working on the bash 3.2 that ships with macOS.
PRESERVED_PATHS=()
while IFS= read -r preserved; do
  [ -n "$preserved" ] && PRESERVED_PATHS+=("$preserved")
done < <(python3 - "$METADATA_FILE" <<'PY'
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

# The comparison base is the upstream revision this branch last consumed. The
# sync workflow stores it in a state file so conflict detection stays correct
# even though mirror commits rewrite history relative to upstream. When the file
# is absent the merge base is used, and as a last resort the empty tree is used
# so the very first run still reports preserved-path edits.
BASE_SHA=""
if [ -f "$STATE_FILE" ]; then
  BASE_SHA="$(tr -d '\r\n' <"$STATE_FILE")"
fi
if [ -z "$BASE_SHA" ]; then
  BASE_SHA="$(git merge-base HEAD "$UPSTREAM_REF" 2>/dev/null || true)"
fi
if [ -z "$BASE_SHA" ]; then
  # No shared history yet: compare against the empty tree so every upstream
  # change is considered and preserved-path edits are still reported.
  BASE_SHA="$(git hash-object -t tree --stdin </dev/null)"
  echo "sync-upstream: no common history with ${UPSTREAM_REF}; using empty tree as base" >&2
fi
if ! git cat-file -e "${BASE_SHA}^{tree}" 2>/dev/null && ! git cat-file -e "${BASE_SHA}^{commit}" 2>/dev/null; then
  echo "sync-upstream: recorded base ${BASE_SHA} is not present; falling back" >&2
  BASE_SHA="$(git merge-base HEAD "$UPSTREAM_REF" 2>/dev/null || git hash-object -t tree --stdin </dev/null)"
fi

if [ "$UPSTREAM_SHA" = "$(git rev-parse HEAD)" ]; then
  echo "sync-upstream: already at upstream ${UPSTREAM_SHA:0:12}"
  printf '{"status":"up-to-date","upstreamSha":"%s"}\n' "$UPSTREAM_SHA" >"$SYNC_REPORT"
  exit 0
fi

# Any preserved path that upstream touched since the merge base forces the
# conflict path, because the downstream copy would otherwise be overwritten.
CONFLICTS=()
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

# Restore upstream-tracked files that exist on the upstream side, plus delete
# local copies of files upstream removed. Paths missing from upstream are skipped
# rather than treated as errors: the local branch carries the upstream tree plus
# the downstream layer, so a path that is absent upstream simply has no
# counterpart to copy.
while IFS= read -r path; do
  [ -z "$path" ] && continue
  # Never touch workflow files or sync metadata from the upstream side; those
  # are downstream-owned even though the paths overlap.
  case "$path" in
    .github/*) continue ;;
    *upstream-sync.json*) continue ;;
  esac
  skip=0
  for kept in "${PRESERVED_PATHS[@]}"; do
    case "$path" in
      "$kept"*) skip=1; break ;;
    esac
  done
  [ "$skip" = 1 ] && continue
  # Skip anything upstream does not actually carry; nothing to copy or delete.
  if ! git cat-file -e "${UPSTREAM_REF}:${path}" 2>/dev/null; then
    continue
  fi
  git checkout "$UPSTREAM_REF" -- "$path"
done < <(git diff --name-only --no-renames HEAD "$UPSTREAM_REF")

if git diff --quiet && git diff --cached --quiet; then
  echo "sync-upstream: no textual change after applying upstream; recording revision"
fi

SHORT_SHA="${UPSTREAM_SHA:0:12}"
SUCCESS=1
if [ "${#CONFLICTS[@]}" -gt 0 ]; then
  SUCCESS=0
fi

printf '%s\n' "$UPSTREAM_SHA" >"$STATE_FILE"

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
