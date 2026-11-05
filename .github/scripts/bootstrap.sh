#!/usr/bin/env bash
# =============================================================================
# bootstrap.sh - prepare a fresh clone of this distribution
# =============================================================================
# Purpose
#   One-time setup for a new clone or for the repository owner before the first
#   push: configures the upstream remote, installs the repository hooks and
#   reports whether the required GitHub configuration exists.
#
# Contract
#   - Idempotent: safe to run repeatedly.
#   - Never pushes, never rewrites history, never touches secrets.
#   - Prints a checklist of the GitHub settings that must be enabled manually.
# =============================================================================

set -euo pipefail

REPO_ROOT="${REPO_ROOT:-$(git rev-parse --show-toplevel)}"
cd "$REPO_ROOT"

UPSTREAM_URL="$(python3 - .upstream-sync.json <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["upstream"]["gitUrl"])
PY
)"

echo "==> Configuring the upstream remote"
git remote remove upstream 2>/dev/null || true
git remote add upstream "$UPSTREAM_URL"
git fetch --no-tags upstream main
git remote -v

echo
echo "==> Installing repository hooks"
if [ -d .githooks ]; then
  git config core.hooksPath .githooks
  echo "core.hooksPath set to .githooks"
fi

echo
echo "==> Repository configuration checklist"
cat <<'CHECKLIST'
The following settings are applied in GitHub, not in the repository:

  [ ] Repository visibility chosen (private or public)
  [ ] Actions enabled: Settings -> Actions -> General
  [ ] Workflow permissions: read and write, and allow GitHub Actions to create
      and approve pull requests (required by the sync workflow)
  [ ] Default branch set to main
  [ ] Branch protection on main:
        - require the Hardened CI and CI workflows to pass
        - require at least one approval for the sync pull requests
        - disallow force pushes
  [ ] Labels created: upstream-sync, sync-conflict, sync-automerge
  [ ] Package visibility for ghcr.io/cranly-labs/cranly-router set to the
      intended scope (Settings -> Packages)

Secrets are optional. The workflows use the built-in GITHUB_TOKEN; add any
optional integration secrets yourself and never commit them.
CHECKLIST

echo
echo "==> Bootstrap complete"
