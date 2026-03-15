#!/usr/bin/env python3
"""Generate a CHANGELOG.md entry from git history between two refs.

Usage:
    # Preview the entry for the next release:
    python scripts/generate_changelog.py v0.4.3 HEAD --version 0.4.4

    # Write it directly into CHANGELOG.md (replaces [Unreleased] contents):
    python scripts/generate_changelog.py v0.4.3 HEAD --version 0.4.4 --write

    # Preview what would go into [Unreleased]:
    python scripts/generate_changelog.py v0.4.3 HEAD

Commit prefix mapping:
    feat:      → Added
    fix:       → Fixed
    refactor:  → Changed
    docs:      → Changed
    chore:     → Changed
    style:     → Changed  (excluded by default, use --include-style)
    BREAKING:  → Changed  (any commit containing BREAKING in subject)
    revert:    → Removed
    deprecate: → Deprecated
    security:  → Security

Merge commits and commits with no recognized prefix are skipped.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CHANGELOG_PATH = REPO_ROOT / "CHANGELOG.md"

# Prefix → changelog section.  Order matters for output.
SECTION_ORDER = ["Added", "Changed", "Fixed", "Deprecated", "Removed", "Security"]

PREFIX_MAP: dict[str, str] = {
    "feat": "Added",
    "fix": "Fixed",
    "refactor": "Changed",
    "docs": "Changed",
    "chore": "Changed",
    "style": "Changed",
    "revert": "Removed",
    "deprecate": "Deprecated",
    "security": "Security",
}

# Patterns that indicate merge/housekeeping commits to skip.
SKIP_PATTERNS = [
    re.compile(r"^Merge "),
    re.compile(r"^Merge pull request"),
    re.compile(r"^Merge branch"),
]


def git_log_subjects(from_ref: str, to_ref: str) -> list[str]:
    """Return one-line commit subjects between two refs (oldest first)."""
    result = subprocess.run(
        ["git", "log", "--reverse", "--format=%s", f"{from_ref}..{to_ref}"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=True,
    )
    return [line for line in result.stdout.strip().splitlines() if line]


def classify(
    subjects: list[str], *, include_style: bool = False
) -> dict[str, list[str]]:
    """Map commit subjects into changelog sections."""
    sections: dict[str, list[str]] = defaultdict(list)
    seen: set[str] = set()

    for raw in subjects:
        # Skip merge commits.
        if any(p.match(raw) for p in SKIP_PATTERNS):
            continue

        # Parse prefix.
        m = re.match(r"^(\w+)(?:\(.+?\))?:\s*(.+)$", raw)
        if not m:
            continue

        prefix = m.group(1).lower()
        message = m.group(2).strip()

        if prefix == "style" and not include_style:
            continue

        section = PREFIX_MAP.get(prefix)
        if section is None:
            continue

        # Check for BREAKING.
        if "BREAKING" in raw:
            section = "Changed"

        # Deduplicate by normalized message.
        norm = message.lower()
        if norm in seen:
            continue
        seen.add(norm)

        # Capitalize first letter, ensure no trailing period.
        message = message[0].upper() + message[1:]
        message = message.rstrip(".")

        sections[section].append(f"- {message}")

    return sections


def render(
    sections: dict[str, list[str]],
    *,
    version: str | None = None,
    date: str | None = None,
) -> str:
    """Render a single changelog release block."""
    if version:
        date = date or datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        header = f"## [{version}] - {date}"
    else:
        header = "## [Unreleased]"

    parts = [header, ""]
    for section in SECTION_ORDER:
        parts.append(f"### {section}")
        parts.append("")
        items = sections.get(section, [])
        if items:
            parts.extend(items)
        else:
            parts.append("- None.")
        parts.append("")

    return "\n".join(parts)


def write_changelog(entry: str, *, version: str | None) -> None:
    """Insert the generated entry into CHANGELOG.md.

    If --version is given, replaces the [Unreleased] body with empty
    sections and inserts the versioned entry below it.  Otherwise,
    replaces the [Unreleased] body in place.
    """
    text = CHANGELOG_PATH.read_text()

    # Find [Unreleased] block boundaries.
    unreleased_re = re.compile(
        r"(## \[Unreleased\]\n)(.*?)(?=\n## \[)", re.DOTALL
    )
    m = unreleased_re.search(text)
    if not m:
        print("ERROR: Could not locate ## [Unreleased] section.", file=sys.stderr)
        sys.exit(1)

    empty_unreleased = (
        "## [Unreleased]\n\n"
        "### Added\n\n- None.\n\n"
        "### Changed\n\n- None.\n\n"
        "### Fixed\n\n- None.\n\n"
        "### Deprecated\n\n- None.\n\n"
        "### Removed\n\n- None.\n\n"
        "### Security\n\n- None.\n\n"
    )

    if version:
        replacement = empty_unreleased + entry + "\n"
    else:
        replacement = entry + "\n"

    text = text[: m.start()] + replacement + text[m.end() :]
    CHANGELOG_PATH.write_text(text)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a CHANGELOG.md entry from git history."
    )
    parser.add_argument("from_ref", help="Start ref (exclusive), e.g. v0.4.3")
    parser.add_argument("to_ref", help="End ref (inclusive), e.g. HEAD")
    parser.add_argument(
        "--version",
        help="Version string for the release (e.g. 0.4.4). "
        "Omit to preview as [Unreleased].",
    )
    parser.add_argument(
        "--date",
        help="Release date (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Write the entry into CHANGELOG.md instead of printing to stdout.",
    )
    parser.add_argument(
        "--include-style",
        action="store_true",
        help="Include style: commits (excluded by default).",
    )
    args = parser.parse_args()

    subjects = git_log_subjects(args.from_ref, args.to_ref)
    if not subjects:
        print(f"No commits found between {args.from_ref} and {args.to_ref}.")
        sys.exit(0)

    sections = classify(subjects, include_style=args.include_style)
    entry = render(sections, version=args.version, date=args.date)

    if args.write:
        write_changelog(entry, version=args.version)
        label = args.version or "Unreleased"
        print(f"Wrote [{label}] entry to {CHANGELOG_PATH}")
    else:
        print(entry)


if __name__ == "__main__":
    main()
