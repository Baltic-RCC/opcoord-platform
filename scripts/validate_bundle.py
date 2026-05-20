#!/usr/bin/env python3
"""Validate a CROSA process bundle zip archive."""

import argparse
import json
import sys
import tempfile
import zipfile
from pathlib import Path

REQUIRED_FILES = [
    "config.json",
    "i18n.json",
    "css/style.css",
    "template/sar.handlebars",
]

REQUIRED_DIRS = ["css", "template"]


def find_bundle_root(extracted_dir: Path, bundle_path: str) -> Path:
    top_level = [p for p in extracted_dir.iterdir() if p.is_dir()]
    if len(top_level) != 1:
        raise ValueError(f"Expected 1 top-level directory in archive, found: {[p.name for p in top_level]}")

    root = top_level[0]

    if bundle_path:
        root = root / bundle_path
        if not root.exists():
            raise FileNotFoundError(f"Bundle path '{bundle_path}' not found inside archive")

    return root


def check_dirs(root: Path) -> list[str]:
    return [f"Missing directory: {d}/" for d in REQUIRED_DIRS if not (root / d).is_dir()]


def check_files(root: Path) -> list[str]:
    return [f"Missing file: {f}" for f in REQUIRED_FILES if not (root / f).is_file()]


def check_json(root: Path) -> list[str]:
    errors = []
    for name in ["config.json", "i18n.json"]:
        path = root / name
        if path.is_file():
            try:
                with open(path) as fh:
                    json.load(fh)
            except json.JSONDecodeError as e:
                errors.append(f"Invalid JSON in {name}: {e}")
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate a CROSA process bundle zip archive")
    parser.add_argument("zip_path", help="Path to the bundle zip archive")
    parser.add_argument("--bundle-path", default="", help="Subdirectory path within the archive to validate")
    args = parser.parse_args()

    zip_path = Path(args.zip_path)
    if not zip_path.is_file():
        print(f"[FAIL] Archive not found: {zip_path}")
        sys.exit(1)

    all_errors: list[str] = []

    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp)

        try:
            bundle_root = find_bundle_root(Path(tmp), args.bundle_path)
        except (ValueError, FileNotFoundError) as e:
            print(f"[FAIL] {e}")
            sys.exit(1)

        print(f"Bundle root: {bundle_root.relative_to(tmp)}\n")

        for label, errors in [
            ("Directory structure", check_dirs(bundle_root)),
            ("Required files", check_files(bundle_root)),
            ("JSON validity", check_json(bundle_root)),
        ]:
            if errors:
                print(f"[FAIL] {label}:")
                for err in errors:
                    print(f"       - {err}")
                all_errors.extend(errors)
            else:
                print(f"[PASS] {label}")

    if all_errors:
        print(f"\n{len(all_errors)} validation error(s) found.")
        sys.exit(1)

    print("\nAll validation checks passed.")


if __name__ == "__main__":
    main()
