#!/usr/bin/env bash
# =============================================================================
# resolve-version.sh - resolve the release version for a downstream build
# =============================================================================
# Purpose
#   Emits the version string that is baked into the binary and the container
#   image. The resolution order is: explicit VERSION environment, the exact git
#   tag on HEAD, the upstream VERSION file, then a content hash fallback.
#
# Contract
#   - Always prints a single non-empty line on stdout.
#   - Never fails on a dirty worktree; the fallback hash keeps local builds
#     distinguishable from release builds.
# =============================================================================

set -euo pipefail

if [ -n "${VERSION:-}" ]; then
  printf '%s\n' "${VERSION#v}"
  exit 0
fi

if tag="$(git describe --tags --exact-match 2>/dev/null)"; then
  printf '%s\n' "${tag#v}"
  exit 0
fi

if [ -f backend/cmd/server/VERSION ]; then
  printf '%s\n' "$(tr -d '\r\n' < backend/cmd/server/VERSION)"
  exit 0
fi

printf '0.0.0-dev.%s\n' "$(git rev-parse --short=7 HEAD 2>/dev/null || echo local)"
