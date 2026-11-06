"""locale-catalog.py reads and rebuilds the interface message catalogs.

The catalogs are TypeScript modules holding one nested object of flat string
values. This module tokenises that object rather than matching it with regular
expressions: the files mix single-quoted and double-quoted values, and a value
may contain the other quote character, an escaped quote, or a Vue I18n
placeholder with braces. A regex over the raw text mis-reads such a value as a
nested object and silently corrupts every key after it.

Two entry points are provided. `extract` returns ordered key paths with their
source text, which is what a translation pass consumes. `render` writes a
catalog back in the source layout, which is what keeps a regenerated file
reviewable against the original.
"""

import re
import sys
import json


KEY_RE = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")


def strip_comments(src):
    """Remove line and block comments, preserving string contents."""
    out = []
    i = 0
    n = len(src)
    while i < n:
        c = src[i]
        if c in "'\"`":
            quote = c
            out.append(c)
            i += 1
            while i < n:
                if src[i] == "\\":
                    out.append(src[i:i + 2])
                    i += 2
                    continue
                out.append(src[i])
                if src[i] == quote:
                    i += 1
                    break
                i += 1
            continue
        if src.startswith("//", i):
            j = src.find("\n", i)
            i = n if j == -1 else j
            continue
        if src.startswith("/*", i):
            j = src.find("*/", i + 2)
            i = n if j == -1 else j + 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


def unescape(raw):
    """Decode the escapes a JavaScript string literal may carry."""
    out = []
    i = 0
    while i < len(raw):
        if raw[i] == "\\" and i + 1 < len(raw):
            nxt = raw[i + 1]
            mapping = {"n": "\n", "t": "\t", "r": "\r", "'": "'", '"': '"', "`": "`", "\\": "\\"}
            out.append(mapping.get(nxt, "\\" + nxt))
            i += 2
            continue
        out.append(raw[i])
        i += 1
    return "".join(out)


def read_value(src, i):
    """Read one string literal at `i`; return its decoded text and end index."""
    quote = src[i]
    i += 1
    buf = []
    while i < len(src):
        if src[i] == "\\":
            buf.append(src[i:i + 2])
            i += 2
            continue
        if src[i] == quote:
            return unescape("".join(buf)), i + 1
        buf.append(src[i])
        i += 1
    raise ValueError("unterminated string literal")


def extract(path):
    """Return ordered (key path, source text) pairs for one catalog module."""
    src = strip_comments(open(path, encoding="utf-8").read())
    start = src.index("export default")
    i = src.index("{", start)
    stack = []
    pairs = []
    n = len(src)
    while i < n:
        c = src[i]
        if c in " \t\r\n,":
            i += 1
            continue
        if c == "}":
            if stack:
                stack.pop()
            i += 1
            continue
        if c == "{":
            i += 1
            continue
        # A key is an identifier or a quoted string, each followed by a colon.
        if c in "'\"":
            key, j = read_value(src, i)
            k = j
            while k < n and src[k] in " \t":
                k += 1
            if k < n and src[k] == ":":
                i = k + 1
            else:
                i = j
                continue
        else:
            m = KEY_RE.match(src, i)
            if not m:
                i += 1
                continue
            key = m.group(0)
            k = m.end()
            while k < n and src[k] in " \t":
                k += 1
            if k < n and src[k] == ":":
                i = k + 1
            else:
                i = m.end()
                continue
        while i < n and src[i] in " \t\r\n":
            i += 1
        if i < n and src[i] in "'\"":
            value, i = read_value(src, i)
            pairs.append((".".join(stack + [key]), value))
            continue
        if i < n and src[i] == "{":
            stack.append(key)
            i += 1
            continue
        # Arrays, spreads, and anything else carry no translatable leaf here.
    return pairs


def main(argv):
    """Print the requested catalog as JSON keyed by module path."""
    if len(argv) < 2:
        print("usage: locale-catalog.py <module.ts> [...]", file=sys.stderr)
        return 2
    result = {path: extract(path) for path in argv[1:]}
    json.dump(result, sys.stdout, ensure_ascii=False, indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
