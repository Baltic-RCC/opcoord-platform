#!/usr/bin/env python3
"""Sync CROSA bundle from GitLab and open a GitHub PR."""

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from validate_bundle import check_dirs, check_files, check_json, find_bundle_root

REPO_ROOT = Path(__file__).parent.parent
CROSA_DIR = REPO_ROOT / "resources" / "process_bundles" / "crosa"


def load_config() -> dict:
    keys = ["GITLAB_TOKEN", "GITLAB_BASE_URL", "GITLAB_PROJECT_ID", "GITLAB_BUNDLE_PATH"]
    config = {k: os.environ.get(k) for k in keys}
    missing = [k for k, v in config.items() if not v]
    if missing:
        print(f"[ERROR] Missing environment variables: {', '.join(missing)}")
        print("Set them in your shell or add them to config/.env and export them.")
        sys.exit(1)
    return config


def download(config: dict, ref: str) -> Path:
    url = (
        f"{config['GITLAB_BASE_URL']}/api/v4/projects/{config['GITLAB_PROJECT_ID']}"
        f"/repository/archive.zip?sha={ref}&path={config['GITLAB_BUNDLE_PATH']}"
    )
    print(f"Downloading bundle from GitLab (ref: {ref})...")
    response = requests.get(url, headers={"PRIVATE-TOKEN": config["GITLAB_TOKEN"]}, stream=True, verify=False)
    response.raise_for_status()
    tmp = Path(tempfile.mktemp(suffix=".zip"))
    with open(tmp, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded: {tmp}")
    return tmp


def validate(zip_path: Path, bundle_path: str) -> None:
    print("\nValidating bundle...")
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp)
        try:
            bundle_root = find_bundle_root(Path(tmp), bundle_path)
        except (ValueError, FileNotFoundError) as e:
            print(f"[FAIL] {e}")
            sys.exit(1)

        print(f"Bundle root: {bundle_root.relative_to(tmp)}\n")
        all_errors = []
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


def extract(zip_path: Path, bundle_path: str) -> None:
    print(f"\nExtracting into {CROSA_DIR}...")
    with tempfile.TemporaryDirectory() as tmp:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp)
        bundle_root = find_bundle_root(Path(tmp), bundle_path)

        if CROSA_DIR.exists():
            shutil.rmtree(CROSA_DIR)
        CROSA_DIR.mkdir(parents=True)

        for item in bundle_root.iterdir():
            dest = CROSA_DIR / item.name
            shutil.copytree(item, dest) if item.is_dir() else shutil.copy2(item, dest)

    print(f"Extracted to {CROSA_DIR}")


def git(*args) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, check=True)


def has_changes() -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain", "resources/process_bundles/crosa"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    return bool(result.stdout.strip())


def get_remote_url() -> str:
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def open_pr(ref: str, timestamp: str) -> None:
    branch = f"chore/sync-crosa-bundle-{timestamp}"
    git("checkout", "-b", branch)
    git("add", "resources/process_bundles/crosa/")
    git("commit", "-m", f"chore: sync crosa bundle from gitlab [{ref}]")
    git("push", "origin", branch)

    remote = get_remote_url().removesuffix(".git")
    pr_url = f"{remote}/compare/main...{branch}?expand=1"

    if shutil.which("gh"):
        body = (
            f"## CROSA Bundle Sync\n\n"
            f"Synced from GitLab ref: `{ref}`\n"
            f"Run timestamp: `{timestamp}`\n\n"
            f"### Validation results\n"
            f"All checks passed:\n"
            f"- Directory structure verified\n"
            f"- Required files present (config.json, i18n.json, css/style.css, template/sar.handlebars)\n"
            f"- JSON files valid"
        )
        subprocess.run(
            ["gh", "pr", "create", "--title", "chore: sync crosa bundle from gitlab",
             "--body", body, "--base", "main", "--head", branch],
            cwd=REPO_ROOT, check=True,
        )
    else:
        import webbrowser
        print(f"\ngh CLI not found. Opening PR page in browser...")
        print(f"PR URL: {pr_url}")
        webbrowser.open(pr_url)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync CROSA bundle from GitLab and open a GitHub PR")
    parser.add_argument("--ref", default="initial-crosa-import", help="GitLab branch or tag (default: initial-crosa-import)")
    args = parser.parse_args()

    config = load_config()
    timestamp = datetime.now().strftime("%Y%m%d%H%M")
    zip_path = None

    try:
        zip_path = download(config, args.ref)
        validate(zip_path, config["GITLAB_BUNDLE_PATH"])
        extract(zip_path, config["GITLAB_BUNDLE_PATH"])
    finally:
        if zip_path and zip_path.exists():
            zip_path.unlink()

    if not has_changes():
        print("\nBundle is already up to date, nothing to commit.")
        return

    print("\nChanges detected, opening PR...")
    open_pr(args.ref, timestamp)
    print("\nDone! PR opened on GitHub.")


if __name__ == "__main__":
    main()
