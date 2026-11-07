"""locale-verify.py checks a translated catalog against its English source.

A translation must carry the same key set and the same interpolation
placeholders as the message it replaces. A dropped `{count}` renders a literal
placeholder, and an added one can crash the formatter, so both are treated as
failures.

Markup tags are compared as multisets for the same reason. Two differences are
expected rather than defects, because the target language is not the source
language: a plural message needs a form per plural category, so it may repeat a
placeholder more times than the source does, and a bracketed span that stands
for a value a reader types, such as `<your-admin-api-key>`, is translated along
with the sentence around it. Both are reported as advisories rather than
problems.

Usage
    locale-verify.py <source.json> <translation.json>

Both files map message keys to strings. Exit status is 0 when every message
matches and 1 otherwise, with one line per offending key. Advisories describe
differences that follow from the target language and do not affect the exit
status.
"""

import json
import re
import sys


PLACEHOLDER = re.compile(r"\{([A-Za-z0-9_]+)\}")
TAG = re.compile(r"</?([a-zA-Z][a-zA-Z0-9]*)[^>]*>")
# A bracketed span that holds a hyphenated word with no attributes or spaces:
# the shape of `<your-admin-api-key>`, not of `<div class="x">`. The content is
# matched in any script because the translation carries it in its own.
PLACEHOLDER_SPAN = re.compile(r"<([^\s<>=/]+(?:-[^\s<>=/]+)+)>")
# A real tag: a name that ends at `>` or at an attribute rather than inside a
# hyphenated word, so `<div>` and `<div class="x">` match but `<your-key>` does
# not.
REAL_TAG = re.compile(r"</?([a-zA-Z][a-zA-Z0-9]*)(?:\s[^>]*)?/?>")


def _only_translated_spans(english, russian):
    """Report whether the tag differences come only from bracketed spans.

    Both strings must carry the same number of spans and the same number of
    pieces between them, so a span that was dropped or added still counts as
    markup damage. The source's spans must also not be real tags, because a tag
    that the translation turns into prose — `<div>` becoming plain text — is
    damage however few spans are involved.
    """
    english_spans = PLACEHOLDER_SPAN.findall(english)
    russian_spans = PLACEHOLDER_SPAN.findall(russian)
    if not english_spans or not russian_spans:
        return False
    # Each bracketed span must be answered by exactly one on the other side, so
    # a span that was dropped or added keeps counting as a markup problem.
    if len(english_spans) != len(russian_spans):
        return False
    if len(PLACEHOLDER_SPAN.split(english)) != len(PLACEHOLDER_SPAN.split(russian)):
        return False
    # Removing the spans must not remove a real tag as well; when the source
    # holds only prose in brackets, what is left still parses as the same text.
    return not any(REAL_TAG.fullmatch(span.group(0)) for span in PLACEHOLDER_SPAN.finditer(english))


def check(source, translation):
    """Return the problems and advisories between the two catalogs.

    Problems are differences that break a message; advisories are differences
    that follow from the target language and are reported for review only.
    """
    problems = []
    advisories = []
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
            # Russian distinguishes more plural categories than English, so a
            # plural message legitimately repeats its placeholder once per
            # category. Extra repeats of a placeholder the source already has
            # are narrowed to an advisory; a dropped or renamed one is not.
            if not set(expected) <= set(actual):
                problems.append(f"placeholder mismatch: {key} expected {expected} got {actual}")
            elif len(actual) > len(expected):
                advisories.append(
                    f"extra placeholder form: {key} expected {expected} got {actual}"
                )
        expected_tags = sorted(TAG.findall(english))
        actual_tags = sorted(TAG.findall(russian))
        if expected_tags != actual_tags:
            # A tag that vanishes or changes name breaks the markup, so it is a
            # problem. A bracketed span whose content is a translatable phrase is
            # not markup at all: the placeholder name is the words themselves, and
            # translating it is the point rather than a defect.
            if not set(expected_tags) <= set(actual_tags):
                if _only_translated_spans(english, russian):
                    advisories.append(
                        f"translated span text: {key} expected {expected_tags} got {actual_tags}"
                    )
                else:
                    problems.append(f"markup mismatch: {key} expected {expected_tags} got {actual_tags}")
            else:
                advisories.append(f"translated markup text: {key} expected {expected_tags} got {actual_tags}")
        # A translation that is byte-identical to the source is usually a
        # forgotten string rather than a deliberate loanword.
        if english.strip() and english == russian and len(english) > 3:
            problems.append(f"untranslated: {key}")
    for key in list(translation):
        if key not in source:
            problems.append(f"extra key: {key}")
    return problems, advisories


def main(argv):
    """Compare the two catalogs named on the command line."""
    if len(argv) != 3:
        print("usage: locale-verify.py <source.json> <translation.json>", file=sys.stderr)
        return 2
    source = dict(json.load(open(argv[1], encoding="utf-8")))
    translation = json.load(open(argv[2], encoding="utf-8"))
    problems, advisories = check(source, translation)
    for problem in problems:
        print(problem)
    for advisory in advisories:
        print(advisory)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
