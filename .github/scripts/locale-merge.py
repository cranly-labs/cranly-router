"""locale-merge.py keeps a downstream locale entry in an upstream-owned manifest.

The frontend and backend share one locale manifest, and the upstream
synchronisation copier rewrites that file from upstream on every run. Adding the
downstream locale directly would be reverted by the next sync, while preserving
the whole file would turn every upstream manifest edit into a manual merge.

This script takes the third option. It reads whatever upstream currently ships,
appends the declared downstream locale entries that are absent, and writes the
result back. Running it on an already-merged manifest changes nothing, so it is
safe to call on every sync and after any checkout.

Usage
    locale-merge.py <manifest> <locales-json>

`locales-json` is a JSON array of locale definitions in the manifest's own shape.
Entries whose code already exists are left untouched, so an upstream locale with
the same code always wins.
"""

import json
import sys


def render(manifest):
    """Render the manifest with one locale per line.

    The upstream file writes each locale as a single compact line. Re-serialising
    with a standard indenting dump would expand those lines and turn every sync
    into a whole-file rewrite, so the layout is reproduced here and only the
    locale lines are regenerated.
    """
    lines = ['{', '  "default": %s,' % json.dumps(manifest.get("default", "en")), '  "locales": [']
    entries = manifest.get("locales", [])
    for index, entry in enumerate(entries):
        # Upstream writes `{ "code": ... }` with a space inside the braces, so
        # the rendered line reproduces that shape and a merged file differs from
        # upstream only by the added locale line.
        rendered = json.dumps(entry, ensure_ascii=False, separators=(", ", ": "))
        rendered = "{ " + rendered[1:-1] + " }"
        comma = "," if index < len(entries) - 1 else ""
        lines.append("    %s%s" % (rendered, comma))
    lines.append("  ]")
    lines.append("}")
    return "\n".join(lines) + "\n"


def merge(manifest_path, locales_path):
    """Add missing downstream locales and report the codes appended."""
    with open(manifest_path, encoding="utf-8") as handle:
        manifest = json.load(handle)
    with open(locales_path, encoding="utf-8") as handle:
        wanted = json.load(handle)

    existing = {entry["code"] for entry in manifest.get("locales", [])}
    added = [entry for entry in wanted if entry["code"] not in existing]
    if not added:
        return []

    manifest["locales"].extend(added)
    with open(manifest_path, "w", encoding="utf-8") as handle:
        handle.write(render(manifest))
    return [entry["code"] for entry in added]


def main(argv):
    """Run the merge and print the appended locale codes, one per line."""
    if len(argv) != 3:
        print("usage: locale-merge.py <manifest> <locales-json>", file=sys.stderr)
        return 2
    for code in merge(argv[1], argv[2]):
        print(code)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
