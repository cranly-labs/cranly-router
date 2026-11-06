"""locale-verify.py checks a translated catalog against its English source.

A translation must carry the same key set and the same interpolation
placeholders as the message it replaces. A dropped `{count}` renders a literal
placeholder, and an added one can crash the formatter, so both are treated as
failures. Markup tags are compared as multisets for the same reason.

Usage
    locale-verify.py <source.json> <translation.json>

Both files map message keys to strings. Exit status is 0 when every message
matches and 1 otherwise, with one line per offending key.
"""

import json
import re
import sys


PLACEHOLDER = re.compile(r"\{([A-Za-z0-9_]+)\}")
TAG = re.compile(r"</?([a-zA-Z][a-zA-Z0-9]*)[^>]*>")


def check(source, translation):
    """Return a list of human-readable problems between the two catalogs."""
    problems = []
    for key, english in source.items():
        if key not in translation:
            problems.append(f"missing key: {key}")
            continue
        russian = translation[key]
        if not isinstance(russian, str):
            problems.append(f"not a string: {key}")
            continue
        if not russian.strip():
            problems.append(f"empty value: {key}")
            continue
        expected = sorted(PLACEHOLDER.findall(english))
        actual = sorted(PLACEHOLDER.findall(russian))
        if expected != actual:
            problems.append(f"placeholder mismatch: {key} expected {expected} got {actual}")
        expected_tags = sorted(TAG.findall(english))
        actual_tags = sorted(TAG.findall(russian))
        if expected_tags != actual_tags:
            problems.append(f"markup mismatch: {key} expected {expected_tags} got {actual_tags}")
        # A translation that is byte-identical to the source is usually a
        # forgotten string rather than a deliberate loanword.
        if english.strip() and english == russian and len(english) > 3:
            problems.append(f"untranslated: {key}")
    for key in translation:
        if key not in source:
            problems.append(f"extra key: {key}")
    return problems


def main(argv):
    """Compare the two catalogs named on the command line."""
    if len(argv) != 3:
        print("usage: locale-verify.py <source.json> <translation.json>", file=sys.stderr)
        return 2
    source = dict(json.load(open(argv[1], encoding="utf-8")))
    translation = json.load(open(argv[2], encoding="utf-8"))
    problems = check(source, translation)
    for problem in problems:
        print(problem)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
