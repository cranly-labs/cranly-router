#!/usr/bin/env bash
# =============================================================================
# version-bump.sh - resolve the downstream release version for the current tree
# =============================================================================
# Purpose
#   Combines the upstream version with a downstream revision counter and prints
#   the release version as X.Y.Z-custom.N. The upstream part is read from
#   backend/cmd/server/VERSION, which the sync pipeline refreshes from upstream,
#   and the revision lives in .github/custom-revision so it survives that sync.
#
# Contract
#   - Prints one version line on stdout and never writes unless --write is given.
#   - With --write, updates .github/custom-revision to match the printed version.
#   - Exits 1 when a version cannot be produced from the inputs it is given.
#
# Environment
#   VERSION_FILE     upstream version file (default: backend/cmd/server/VERSION)
#   REVISION_FILE    downstream revision record (default: .github/custom-revision)
#   SUFFIX           revision label between version and counter (default: custom)
# =============================================================================

set -euo pipefail

MIRROR_ROOT="$(git rev-parse --show-toplevel)"
cd "$MIRROR_ROOT"

VERSION_FILE="${VERSION_FILE:-backend/cmd/server/VERSION}"
REVISION_FILE="${REVISION_FILE:-.github/custom-revision}"
SUFFIX="${SUFFIX:-custom}"

WRITE=0
for arg in "$@"; do
  case "$arg" in
    --write) WRITE=1 ;;
    --dry-run) WRITE=0 ;;
    *)
      echo "version-bump: unknown argument ${arg}" >&2
      exit 1
      ;;
  esac
done

if [ ! -f "$VERSION_FILE" ]; then
  echo "version-bump: missing upstream version file ${VERSION_FILE}" >&2
  exit 1
fi

# The file is the upstream-owned version record, so it is read verbatim rather
# than derived from tags: a mirrored tag would pin the previous release and hide
# an upstream advance that arrived without a tag.
UPSTREAM_VERSION="$(tr -d '\r\n' <"$VERSION_FILE")"
UPSTREAM_VERSION="${UPSTREAM_VERSION#v}"
if [ -z "$UPSTREAM_VERSION" ]; then
  echo "version-bump: ${VERSION_FILE} is empty" >&2
  exit 1
fi

# The counter is carried across releases so repeated downstream fixes to one
# upstream version get distinct versions. A missing record means this is the
# first downstream release for the version in the tree, so it starts at zero and
# the increment below produces revision one.
RECORDED_VERSION=""
RECORDED_REVISION=0
if [ -f "$REVISION_FILE" ]; then
  read -r RECORDED_VERSION RECORDED_REVISION < <(python3 - "$REVISION_FILE" <<'PY'
import json, sys
record = json.load(open(sys.argv[1]))
print(record.get("upstreamVersion", ""), record.get("revision", 0))
PY
)
fi

# A changed upstream version restarts the counter: revision 3 of 0.6.3 says
# nothing about 0.6.4, and carrying it over would misreport how many downstream
# releases exist for the new base.
if [ "$RECORDED_VERSION" = "$UPSTREAM_VERSION" ]; then
  NEXT_REVISION=$((RECORDED_REVISION + 1))
else
  NEXT_REVISION=1
fi

RELEASE_VERSION="${UPSTREAM_VERSION}-${SUFFIX}.${NEXT_REVISION}"

if [ "$WRITE" = 1 ]; then
  python3 - "$REVISION_FILE" "$UPSTREAM_VERSION" "$NEXT_REVISION" <<'PY'
import json, sys
path, version, revision = sys.argv[1:4]
json.dump(
    {"upstreamVersion": version, "revision": int(revision)},
    open(path, "w"),
    indent=2,
)
open(path, "a").write("\n")
PY
fi

printf '%s\n' "$RELEASE_VERSION"
