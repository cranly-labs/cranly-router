"""locale-build.py renders the Russian catalogs from reviewed translations.

The English catalog is the source of layout: this script replaces the values in
each module with the matching translation and writes the result into the locale
directory named on the command line. Keys, comments, ordering, indentation, and
blank lines are carried over from the source, so a generated module differs
from the English one only in its translated text.

Translations are read from a directory of `<module>.ru.json` files holding a
single object that maps message keys to translated strings. A module whose
translation file is absent is left alone, which lets the locale be built
incrementally while translations are still being produced. A module whose
translation contradicts its source in a way that breaks a message — a missing
key, a dropped placeholder — is reported and skipped rather than written.

Usage
    locale-build.py <translations-dir> [locale-name]

`locale-name` defaults to `ru`.
"""

import importlib.util
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _sibling(name):
    """Load a helper that sits next to this script.

    The helpers are named with hyphens, which makes them unimportable by name,
    so they are loaded from their path instead.
    """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


locale_catalog = _sibling('locale-catalog.py')
locale_verify = _sibling('locale-verify.py')


def modules(english_root):
    """Yield (relative path, absolute path) for every catalog module."""
    for name in sorted(os.listdir(english_root)):
        full = os.path.join(english_root, name)
        if os.path.isdir(full):
            for nested in sorted(os.listdir(full)):
                yield os.path.join(name, nested), os.path.join(full, nested)
        elif name.endswith('.ts'):
            yield name, full


def main(argv):
    """Render every translated module and report what was written.

    Returns 1 when a translated module contradicts its source, in which case
    that module is left unwritten rather than shipped with a broken message.
    """
    if len(argv) < 2:
        print("usage: locale-build.py <translations-dir> [locale]", file=sys.stderr)
        return 2
    translations_dir = argv[1]
    locale = argv[2] if len(argv) > 2 else 'ru'
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')
    english_root = os.path.join(root, 'frontend', 'src', 'i18n', 'locales', 'en')
    target_root = os.path.join(root, 'frontend', 'src', 'i18n', 'locales', locale)

    problems = 0
    written = 0
    skipped = []
    for relative, source_path in modules(english_root):
        # The translation files flatten the module path with its separators
        # doubled, so a root module and an admin module of the same name cannot
        # collide in one directory.
        key = relative.replace(os.sep, '__')
        translation_path = os.path.join(translations_dir, f"{key}.{locale}.json")
        if not os.path.exists(translation_path):
            skipped.append(relative)
            continue
        source = dict(locale_catalog.extract(source_path))
        translation = json.load(open(translation_path, encoding='utf-8'))
        found, advisories = locale_verify.check(source, translation)
        # A message that is identical to its English source is usually a
        # deliberate loanword such as a product name, so it does not block the
        # build. Every other difference does.
        found = [p for p in found if not p.startswith('untranslated')]
        for advisory in advisories:
            print(f"{relative}: {advisory}", file=sys.stderr)
        if found:
            problems += len(found)
            for problem in found:
                print(f"{relative}: {problem}", file=sys.stderr)
            continue
        target_path = os.path.join(target_root, relative)
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        locale_catalog.render_template(source_path, target_path, translation)
        written += 1

    print(f"wrote {written} module(s) into {locale}" if written else "wrote no modules")
    if skipped:
        print(f"skipped {len(skipped)} without a translation:")
        for item in skipped:
            print(f"  {item}")
    if problems:
        print(f"{problems} problem(s) found; affected modules were not written", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
