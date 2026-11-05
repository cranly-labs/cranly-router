#!/usr/bin/env bash
# =============================================================================
# sync-tags.sh - mirror upstream release tags into the downstream repository
# =============================================================================
# Purpose
#   Copies every annotated upstream release tag that matches the configured
#   pattern into the mirror and prints the tags that were created during this
#   run so a workflow can fan out a release build per tag.
#
# Contract
#   - Never moves or deletes an existing tag.
#   - Exits 0 when there is nothing to do.
#   - Writes one created tag per line to stdout, in creation order.
#
# Environment
#   MIRROR_ROOT        repository root (default: git toplevel)
#   UPSTREAM_REMOTE    git remote name to fetch (default: upstream)
#   TAG_PATTERN        glob of tags to mirror (default: v*)
#   TAG_SUFFIX         suffix appended to mirrored tags (default: empty)
#   TAG_LIMIT          mirror at most this many tags per run, newest first;
#                      "0" mirrors every tag (default: 0)
#   PUSH_TAGS          "true" pushes created tags to origin (default: false)
# =============================================================================

set -euo pipefail

MIRROR_ROOT="${MIRROR_ROOT:-$(git rev-parse --show-toplevel)}"
cd "$MIRROR_ROOT"

UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-upstream}"
TAG_PATTERN="${TAG_PATTERN:-v*}"
TAG_SUFFIX="${TAG_SUFFIX:-}"
# A first run against a repository with years of history would otherwise mirror
# every upstream tag at once and fan out one image build per tag.
TAG_LIMIT="${TAG_LIMIT:-0}"
PUSH_TAGS="${PUSH_TAGS:-false}"
METADATA_FILE="${MIRROR_ROOT}/.upstream-sync.json"

UPSTREAM_URL="$(python3 - "$METADATA_FILE" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["upstream"]["gitUrl"])
PY
)"

if ! git remote get-url "$UPSTREAM_REMOTE" >/dev/null 2>&1; then
  git remote add "$UPSTREAM_REMOTE" "$UPSTREAM_URL"
fi

git fetch --no-tags --prune "$UPSTREAM_REMOTE" '+refs/tags/*:refs/upstream-tags/*'

# Mirror tags are annotated, and git refuses to create one without a committer
# identity. Check before the loop so a misconfigured runner reports the cause
# instead of failing with a bare exit code partway through mirroring.
if ! git config user.email >/dev/null 2>&1 || [ -z "$(git config user.email)" ]; then
  echo "sync-tags: git user.email is not configured; annotated tags cannot be created" >&2
  exit 1
fi

CREATED=()
# Collect the missing tags first so TAG_LIMIT can select the newest by version
# order. `git for-each-ref` yields refname order, which sorts v0.10.0 before
# v0.9.0, so version ordering is applied here rather than relying on the ref
# listing order.
MISSING=()
while IFS= read -r ref; do
  source_tag="${ref#refs/upstream-tags/}"
  target_tag="${source_tag}${TAG_SUFFIX}"
  case "$source_tag" in
    $TAG_PATTERN) ;;
    *) continue ;;
  esac
  if git rev-parse -q --verify "refs/tags/${target_tag}" >/dev/null; then
    continue
  fi
  MISSING+=("${source_tag}${TAG_SUFFIX}")
done < <(git for-each-ref --format='%(refname)' refs/upstream-tags)

if [ "${#MISSING[@]}" -gt 0 ]; then
  # `sort -V` gives semantic version order. Read the result back with a loop
  # because mapfile requires bash 4, while macOS ships bash 3.2.
  SORTED=()
  while IFS= read -r candidate; do
    [ -n "$candidate" ] && SORTED+=("$candidate")
  done < <(printf '%s\n' "${MISSING[@]}" | sort -V)
  if [ "$TAG_LIMIT" != "0" ] && [ "${#SORTED[@]}" -gt "$TAG_LIMIT" ]; then
    echo "sync-tags: ${#SORTED[@]} tags outstanding; mirroring the newest ${TAG_LIMIT}" >&2
    SORTED=("${SORTED[@]: -$TAG_LIMIT}")
  fi

  for target_tag in "${SORTED[@]}"; do
    source_tag="${target_tag%${TAG_SUFFIX}}"
    ref="refs/upstream-tags/${source_tag}"
    # Peel annotated tags to the commit they point at. Tagging the tag object
    # itself would create a nested tag that git describe and release tooling
    # cannot resolve correctly.
    peeled="$(git rev-parse "${ref}^{commit}")"
    git tag -a "$target_tag" "$peeled" -m "Mirror of upstream tag ${source_tag}"
    CREATED+=("$target_tag")
  done
fi

if [ "${#CREATED[@]}" -eq 0 ]; then
  echo "sync-tags: no new upstream tags" >&2
  exit 0
fi

if [ "$PUSH_TAGS" = "true" ]; then
  git push origin "${CREATED[@]}"
fi

printf '%s\n' "${CREATED[@]}"
