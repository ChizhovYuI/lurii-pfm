#!/usr/bin/env python3
# ruff: noqa: T201, S603, TRY003, EM101, EM102, PLR2004, C901
"""Release automation for lurii-pfm, lurii-finance, and the Homebrew tap.

This script is intentionally opinionated:
- It assumes the three repos live side-by-side in the same workspace.
- It aborts if the targeted repos are dirty. Feature commits should already exist.
- It performs the same steps used for manual releases:
  version bump -> verification -> build assets -> commit -> push -> tag ->
  GitHub release -> tap update.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True)
class RepoPaths:
    pfm_repo: Path
    app_repo: Path
    tap_repo: Path
    pfm_version_file: Path
    pfm_test_file: Path
    app_project_file: Path
    app_export_options: Path
    tap_formula_file: Path
    tap_cask_file: Path
    formula_resource_script: Path


@dataclass(frozen=True)
class ReleaseTarget:
    release_pfm: bool
    release_app: bool
    pfm_version: str | None
    app_version: str | None
    app_build: str | None
    changelog_comment: str
    verify_brew: bool


def main() -> int:
    args = parse_args()
    paths = discover_paths()
    target = prepare_target(paths, args)

    ensure_prerequisites(target)
    verify_release_script(paths)
    ensure_clean_worktree(paths.tap_repo)
    if target.release_pfm:
        ensure_clean_worktree(paths.pfm_repo)
    if target.release_app:
        ensure_clean_worktree(paths.app_repo)

    pfm_asset: Path | None = None
    app_asset: Path | None = None

    if target.release_pfm and target.pfm_version is not None:
        write_pfm_version(paths, target.pfm_version)
    if target.release_app and target.app_version is not None and target.app_build is not None:
        write_app_version(paths, target.app_version, target.app_build)

    if target.release_pfm:
        verify_pfm(paths)
    if target.release_app:
        verify_app(paths)

    if target.release_pfm and target.pfm_version is not None:
        pfm_asset = build_pfm_asset(paths, target.pfm_version)
        commit_repo(
            repo=paths.pfm_repo,
            files=[paths.pfm_version_file],
            message=f"Bump version to {target.pfm_version}",
        )
        push_branch(paths.pfm_repo)
        create_git_tag(paths.pfm_repo, f"v{target.pfm_version}")
        push_tag(paths.pfm_repo, f"v{target.pfm_version}")
        create_github_release(
            repo=paths.pfm_repo,
            remote_repo="ChizhovYuI/lurii-pfm",
            tag=f"v{target.pfm_version}",
            asset=pfm_asset,
            changelog_comment=target.changelog_comment,
        )

    if target.release_app and target.app_version is not None:
        app_asset = build_app_asset(paths, target.app_version)
        commit_repo(
            repo=paths.app_repo,
            files=[paths.app_project_file],
            message=f"Bump version to {target.app_version}",
        )
        push_branch(paths.app_repo)
        create_git_tag(paths.app_repo, f"v{target.app_version}")
        push_tag(paths.app_repo, f"v{target.app_version}")
        create_github_release(
            repo=paths.app_repo,
            remote_repo="ChizhovYuI/lurii-finance",
            tag=f"v{target.app_version}",
            asset=app_asset,
            changelog_comment=target.changelog_comment,
        )

    update_tap(
        paths=paths,
        pfm_version=target.pfm_version,
        pfm_sha=sha256_file(pfm_asset) if pfm_asset is not None else None,
        app_version=target.app_version,
        app_sha=sha256_file(app_asset) if app_asset is not None else None,
    )
    if target.release_pfm:
        sync_formula_resources(paths)
        if target.verify_brew:
            verify_brew_formula(paths)
    commit_repo(
        repo=paths.tap_repo,
        files=[
            path
            for path, enabled in (
                (paths.tap_formula_file, target.release_pfm),
                (paths.tap_cask_file, target.release_app),
            )
            if enabled
        ],
        message=tap_commit_message(target),
    )
    push_branch(paths.tap_repo)

    print()
    if target.release_pfm and target.pfm_version is not None:
        print(f"lurii-pfm release: v{target.pfm_version}")
    if target.release_app and target.app_version is not None:
        print(f"lurii-finance release: v{target.app_version}")
    print("homebrew-lurii tap updated")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--only",
        choices=("all", "pfm", "app"),
        default="all",
        help="Release both repos, or just one of them.",
    )
    parser.add_argument("--pfm-version", help="Explicit lurii-pfm version. Defaults to the next patch version.")
    parser.add_argument("--app-version", help="Explicit lurii-finance version. Defaults to the next patch version.")
    parser.add_argument(
        "--app-build",
        help="Explicit CURRENT_PROJECT_VERSION for lurii-finance. Defaults to the current build number plus 1.",
    )
    parser.add_argument(
        "--changelog-comment",
        help="Required text to prepend to the generated GitHub release notes.",
    )
    parser.add_argument(
        "--verify-brew",
        action="store_true",
        help="Run a local Homebrew smoke-check for lurii-pfm after syncing formula resources.",
    )
    return parser.parse_args()


def discover_paths() -> RepoPaths:
    pfm_repo = Path(__file__).resolve().parents[1]
    workspace = pfm_repo.parent
    app_repo = workspace / "lurii-finance"
    tap_repo = workspace / "homebrew-lurii"

    paths = RepoPaths(
        pfm_repo=pfm_repo,
        app_repo=app_repo,
        tap_repo=tap_repo,
        pfm_version_file=pfm_repo / "src/pfm/__init__.py",
        pfm_test_file=pfm_repo / "tests/test_updates.py",
        app_project_file=app_repo / "lurii-finance.xcodeproj/project.pbxproj",
        app_export_options=app_repo / "ExportOptions.plist",
        tap_formula_file=tap_repo / "Formula/lurii-pfm.rb",
        tap_cask_file=tap_repo / "Casks/lurii-finance.rb",
        formula_resource_script=pfm_repo / "scripts/generate_homebrew_formula_resources.py",
    )

    for path in (
        paths.pfm_repo,
        paths.app_repo,
        paths.tap_repo,
        paths.pfm_version_file,
        paths.app_project_file,
        paths.app_export_options,
        paths.tap_formula_file,
        paths.tap_cask_file,
        paths.formula_resource_script,
    ):
        if not path.exists():
            raise SystemExit(f"Missing required path: {path}")

    return paths


def prepare_target(paths: RepoPaths, args: argparse.Namespace) -> ReleaseTarget:
    release_pfm = args.only in {"all", "pfm"}
    release_app = args.only in {"all", "app"}
    changelog_comment = str(args.changelog_comment or "").strip()

    pfm_version = (
        next_patch_version(read_pfm_version(paths)) if release_pfm and args.pfm_version is None else args.pfm_version
    )
    app_version = (
        next_patch_version(read_app_version(paths)) if release_app and args.app_version is None else args.app_version
    )
    app_build = str(read_app_build(paths) + 1) if release_app and args.app_build is None else args.app_build

    if release_pfm and pfm_version is None:
        raise SystemExit("pfm release selected but no pfm version could be resolved.")
    if release_app and (app_version is None or app_build is None):
        raise SystemExit("app release selected but app version/build could not be resolved.")
    if not changelog_comment:
        raise SystemExit("Release requires --changelog-comment.")

    return ReleaseTarget(
        release_pfm=release_pfm,
        release_app=release_app,
        pfm_version=pfm_version,
        app_version=app_version,
        app_build=app_build,
        changelog_comment=changelog_comment,
        verify_brew=bool(args.verify_brew),
    )


def run(cmd: Sequence[str], *, cwd: Path) -> str:
    print(f"[run] {cwd}: {' '.join(cmd)}")
    completed = subprocess.run(cmd, cwd=cwd, check=True, text=True, capture_output=True)
    stdout = completed.stdout.strip()
    if stdout:
        print(stdout)
    stderr = completed.stderr.strip()
    if stderr:
        print(stderr)
    return stdout


def ensure_prerequisites(target: ReleaseTarget) -> None:
    required_tools = {"git", "gh", "uv", "python3"}
    if target.release_app:
        required_tools.update({"ditto", "xcodebuild"})

    missing = sorted(tool for tool in required_tools if shutil.which(tool) is None)
    if missing:
        raise SystemExit(f"Missing required tools: {', '.join(missing)}")

    try:
        run(["gh", "auth", "status"], cwd=Path.cwd())
    except subprocess.CalledProcessError as exc:
        if exc.returncode != 0:
            raise SystemExit("GitHub CLI is not authenticated. Run `gh auth login` first.") from exc
        raise


def ensure_clean_worktree(repo: Path) -> None:
    status = run(["git", "status", "--short"], cwd=repo)
    if status:
        raise SystemExit(f"Dirty worktree: {repo}")


def current_branch(repo: Path) -> str:
    branch = run(["git", "branch", "--show-current"], cwd=repo).strip()
    if not branch:
        raise SystemExit(f"Unable to resolve current branch for {repo}")
    return branch


def latest_tag(repo: Path) -> str | None:
    tags = [tag.strip() for tag in run(["git", "tag", "--sort=-version:refname"], cwd=repo).splitlines() if tag.strip()]
    return tags[0] if tags else None


def changed_python_files_for_release(repo: Path) -> list[str]:
    changed: set[str] = set()
    tag = latest_tag(repo)
    if tag is not None:
        changed.update(
            path.strip()
            for path in run(
                ["git", "diff", "--name-only", "--diff-filter=ACMRTUXB", f"{tag}..HEAD"],
                cwd=repo,
            ).splitlines()
            if path.strip()
        )
    else:
        changed.update(path.strip() for path in run(["git", "ls-files"], cwd=repo).splitlines() if path.strip())

    changed.update(
        path.strip()
        for path in run(["git", "diff", "--name-only", "--diff-filter=ACMRTUXB"], cwd=repo).splitlines()
        if path.strip()
    )
    return sorted(path for path in changed if path.endswith(".py"))


def mypy_targets_for_release(repo: Path) -> list[str]:
    return sorted(path for path in changed_python_files_for_release(repo) if path.startswith(("src/", "scripts/")))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def read_pfm_version(paths: RepoPaths) -> str:
    match = re.search(r'__version__ = "([^"]+)"', read_text(paths.pfm_version_file))
    if match is None:
        raise SystemExit("Unable to parse pfm version")
    return match.group(1)


def read_app_version(paths: RepoPaths) -> str:
    versions: set[str] = set(re.findall(r"MARKETING_VERSION = ([0-9.]+);", read_text(paths.app_project_file)))
    if len(versions) != 1:
        raise SystemExit(f"Expected one app MARKETING_VERSION, got: {sorted(versions)}")
    return next(iter(versions))


def read_app_build(paths: RepoPaths) -> int:
    builds: set[str] = set(re.findall(r"CURRENT_PROJECT_VERSION = ([0-9]+);", read_text(paths.app_project_file)))
    if len(builds) != 1:
        raise SystemExit(f"Expected one app CURRENT_PROJECT_VERSION, got: {sorted(builds)}")
    return int(next(iter(builds)))


def next_patch_version(version: str) -> str:
    parts = [int(part) for part in version.split(".")]
    if len(parts) == 2:
        parts.append(0)
    parts[-1] += 1
    return ".".join(str(part) for part in parts)


def replace_one(path: Path, pattern: str, replacement: str) -> None:
    content = read_text(path)
    updated, count = re.subn(pattern, replacement, content, count=1, flags=re.MULTILINE)
    if count != 1:
        raise SystemExit(f"Expected one replacement in {path} for {pattern!r}, got {count}")
    write_text(path, updated)


def replace_all(path: Path, pattern: str, replacement: str) -> None:
    content = read_text(path)
    updated, count = re.subn(pattern, replacement, content, flags=re.MULTILINE)
    if count == 0:
        raise SystemExit(f"No replacements in {path} for {pattern!r}")
    write_text(path, updated)


def write_pfm_version(paths: RepoPaths, version: str) -> None:
    replace_one(
        paths.pfm_version_file,
        r'__version__ = "[^"]+"',
        f'__version__ = "{version}"',
    )


def write_app_version(paths: RepoPaths, version: str, build: str) -> None:
    replace_all(paths.app_project_file, r"MARKETING_VERSION = [0-9.]+;", f"MARKETING_VERSION = {version};")
    replace_all(paths.app_project_file, r"CURRENT_PROJECT_VERSION = [0-9]+;", f"CURRENT_PROJECT_VERSION = {build};")


def verify_pfm(paths: RepoPaths) -> None:
    run(["uv", "run", "pytest", "-q", "--no-cov"], cwd=paths.pfm_repo)
    ruff_targets = changed_python_files_for_release(paths.pfm_repo)
    if ruff_targets:
        run(["uv", "run", "ruff", "check", *ruff_targets], cwd=paths.pfm_repo)
    mypy_targets = mypy_targets_for_release(paths.pfm_repo)
    if mypy_targets:
        run(["uv", "run", "mypy", *mypy_targets], cwd=paths.pfm_repo)
    verify_formula_resources(paths)


def verify_release_script(paths: RepoPaths) -> None:
    script_path = str(Path(__file__).resolve().relative_to(paths.pfm_repo))
    run(["uv", "run", "ruff", "check", script_path], cwd=paths.pfm_repo)
    run(["uv", "run", "mypy", script_path], cwd=paths.pfm_repo)
    formula_script_path = str(paths.formula_resource_script.relative_to(paths.pfm_repo))
    run(["uv", "run", "ruff", "check", formula_script_path], cwd=paths.pfm_repo)
    run(["uv", "run", "mypy", formula_script_path], cwd=paths.pfm_repo)


def verify_app(paths: RepoPaths) -> None:
    run(
        [
            "xcodebuild",
            "-project",
            str(paths.app_repo / "lurii-finance.xcodeproj"),
            "-scheme",
            "lurii-finance",
            "-configuration",
            "Debug",
            "-sdk",
            "macosx",
            "build",
        ],
        cwd=paths.app_repo,
    )


def build_pfm_asset(paths: RepoPaths, version: str) -> Path:
    asset = paths.pfm_repo / "dist" / f"lurii_pfm-{version}.tar.gz"
    if asset.exists():
        asset.unlink()
    run(["uv", "build", "--sdist"], cwd=paths.pfm_repo)
    if not asset.exists():
        raise SystemExit(f"Missing built pfm asset: {asset}")
    return asset


def sync_formula_resources(paths: RepoPaths) -> None:
    run(
        [
            "python3",
            str(paths.formula_resource_script),
            "--lock",
            str(paths.pfm_repo / "uv.lock"),
            "--formula",
            str(paths.tap_formula_file),
            "--write",
            "--check",
        ],
        cwd=paths.pfm_repo,
    )


def verify_formula_resources(paths: RepoPaths) -> None:
    run(
        [
            "python3",
            str(paths.formula_resource_script),
            "--lock",
            str(paths.pfm_repo / "uv.lock"),
            "--formula",
            str(paths.tap_formula_file),
            "--check",
        ],
        cwd=paths.pfm_repo,
    )


def verify_brew_formula(paths: RepoPaths) -> None:
    run(["brew", "install", "--build-from-source", str(paths.tap_formula_file)], cwd=paths.pfm_repo)
    run(["/opt/homebrew/bin/pfm", "--help"], cwd=paths.pfm_repo)
    run(
        ["/opt/homebrew/opt/lurii-pfm/libexec/bin/python", "-c", "import pfm.mcp_server"],
        cwd=paths.pfm_repo,
    )


def build_app_asset(paths: RepoPaths, version: str) -> Path:
    archive_root = paths.app_repo / "build" / f"archive-{version}"
    export_root = paths.app_repo / "build" / f"export-{version}"
    asset = paths.app_repo / "build" / f"LuriiFinance-{version}.zip"

    if archive_root.exists():
        shutil.rmtree(archive_root)
    if export_root.exists():
        shutil.rmtree(export_root)
    if asset.exists():
        asset.unlink()

    archive_path = archive_root / "LuriiFinance.xcarchive"

    run(
        [
            "xcodebuild",
            "-project",
            str(paths.app_repo / "lurii-finance.xcodeproj"),
            "-scheme",
            "lurii-finance",
            "-configuration",
            "Release",
            "-archivePath",
            str(archive_path),
            "archive",
        ],
        cwd=paths.app_repo,
    )
    run(
        [
            "xcodebuild",
            "-exportArchive",
            "-archivePath",
            str(archive_path),
            "-exportPath",
            str(export_root),
            "-exportOptionsPlist",
            str(paths.app_export_options),
        ],
        cwd=paths.app_repo,
    )
    exported_app = export_root / "Lurii Finance.app"
    if not exported_app.exists():
        raise SystemExit(f"Missing exported app bundle: {exported_app}")
    run(["ditto", "-c", "-k", "--keepParent", str(exported_app), str(asset)], cwd=paths.app_repo)
    if not asset.exists():
        raise SystemExit(f"Missing app asset: {asset}")
    return asset


def commit_repo(repo: Path, files: Sequence[Path], message: str) -> None:
    relative_files = [str(path.relative_to(repo)) for path in files]
    run(["git", "add", *relative_files], cwd=repo)
    run(["git", "commit", "-m", message], cwd=repo)


def push_branch(repo: Path) -> None:
    branch = current_branch(repo)
    run(["git", "push", "origin", branch], cwd=repo)


def create_git_tag(repo: Path, tag: str) -> None:
    existing = run(["git", "tag", "--list", tag], cwd=repo)
    if existing.strip():
        raise SystemExit(f"Tag already exists in {repo}: {tag}")
    run(["git", "tag", "-a", tag, "-m", tag], cwd=repo)


def push_tag(repo: Path, tag: str) -> None:
    run(["git", "push", "origin", tag], cwd=repo)


def create_github_release(
    repo: Path,
    remote_repo: str,
    tag: str,
    asset: Path,
    changelog_comment: str,
) -> None:
    cmd = [
        "gh",
        "release",
        "create",
        tag,
        str(asset),
        "--repo",
        remote_repo,
        "--title",
        tag,
        "--generate-notes",
    ]
    cmd.extend(["--notes", changelog_comment])
    run(cmd, cwd=repo)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def update_tap(
    *,
    paths: RepoPaths,
    pfm_version: str | None,
    pfm_sha: str | None,
    app_version: str | None,
    app_sha: str | None,
) -> None:
    if pfm_version is not None and pfm_sha is not None:
        replace_one(
            paths.tap_formula_file,
            r'url "https://github.com/ChizhovYuI/lurii-pfm/releases/download/v[^"]+/lurii_pfm-[^"]+\.tar\.gz"',
            f'url "https://github.com/ChizhovYuI/lurii-pfm/releases/download/v{pfm_version}/lurii_pfm-{pfm_version}.tar.gz"',
        )
        replace_one(
            paths.tap_formula_file,
            r'sha256 "[0-9a-f]+"',
            f'sha256 "{pfm_sha}"',
        )

    if app_version is not None and app_sha is not None:
        replace_one(paths.tap_cask_file, r'version "[^"]+"', f'version "{app_version}"')
        replace_one(paths.tap_cask_file, r'sha256 "[0-9a-f]+"', f'sha256 "{app_sha}"')


def tap_commit_message(target: ReleaseTarget) -> str:
    if target.release_pfm and target.release_app and target.pfm_version and target.app_version:
        return f"Bump lurii releases to {target.pfm_version} and {target.app_version}"
    if target.release_pfm and target.pfm_version:
        return f"Bump lurii-pfm to {target.pfm_version}"
    if target.release_app and target.app_version:
        return f"Bump lurii-finance to {target.app_version}"
    raise SystemExit("Nothing selected for tap update")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        if exc.stdout:
            print(exc.stdout)
        if exc.stderr:
            print(exc.stderr, file=sys.stderr)
        raise SystemExit(exc.returncode) from exc
