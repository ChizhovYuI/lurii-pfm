"""Tests for the release automation script."""

from __future__ import annotations

import importlib.util
import sys
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch


def _load_release_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts/release_lurii.py"
    spec = importlib.util.spec_from_file_location("release_lurii", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_prepare_target_carries_verify_brew_flag(tmp_path):
    module = _load_release_module()
    repo = tmp_path / "repo"
    app = tmp_path / "app"
    tap = tmp_path / "tap"
    repo.mkdir()
    app.mkdir()
    tap.mkdir()
    (repo / "src/pfm").mkdir(parents=True)
    (app / "lurii-finance.xcodeproj").mkdir(parents=True)
    (tap / "Formula").mkdir(parents=True)
    (tap / "Casks").mkdir(parents=True)
    (repo / "src/pfm/__init__.py").write_text('__version__ = "0.20.15"\n', encoding="utf-8")
    (app / "lurii-finance.xcodeproj/project.pbxproj").write_text(
        "MARKETING_VERSION = 2.9.9;\nCURRENT_PROJECT_VERSION = 10;\n",
        encoding="utf-8",
    )
    (app / "ExportOptions.plist").write_text("", encoding="utf-8")
    (tap / "Formula/lurii-pfm.rb").write_text("", encoding="utf-8")
    (tap / "Casks/lurii-finance.rb").write_text("", encoding="utf-8")
    (repo / "scripts").mkdir(parents=True)
    (repo / "scripts/generate_homebrew_formula_resources.py").write_text("", encoding="utf-8")

    paths = module.RepoPaths(
        pfm_repo=repo,
        app_repo=app,
        tap_repo=tap,
        pfm_version_file=repo / "src/pfm/__init__.py",
        pfm_test_file=repo / "tests/test_updates.py",
        app_project_file=app / "lurii-finance.xcodeproj/project.pbxproj",
        app_export_options=app / "ExportOptions.plist",
        tap_formula_file=tap / "Formula/lurii-pfm.rb",
        tap_cask_file=tap / "Casks/lurii-finance.rb",
        formula_resource_script=repo / "scripts/generate_homebrew_formula_resources.py",
    )
    args = Namespace(
        only="pfm",
        pfm_version=None,
        app_version=None,
        app_build=None,
        skip_verify=False,
        changelog_comment=None,
        verify_brew=True,
    )

    target = module.prepare_target(paths, args)

    assert target.verify_brew is True


def test_update_tap_updates_formula_version_and_sha(tmp_path):
    module = _load_release_module()
    formula = tmp_path / "lurii-pfm.rb"
    cask = tmp_path / "lurii-finance.rb"
    formula.write_text(
        'url "https://github.com/ChizhovYuI/lurii-pfm/releases/download/v0.20.15/lurii_pfm-0.20.15.tar.gz"\n'
        'sha256 "deadbeef"\n',
        encoding="utf-8",
    )
    cask.write_text('version "2.9.9"\nsha256 "oldappsha"\n', encoding="utf-8")
    paths = module.RepoPaths(
        pfm_repo=tmp_path,
        app_repo=tmp_path,
        tap_repo=tmp_path,
        pfm_version_file=tmp_path / "src/pfm/__init__.py",
        pfm_test_file=tmp_path / "tests/test_updates.py",
        app_project_file=tmp_path / "project.pbxproj",
        app_export_options=tmp_path / "ExportOptions.plist",
        tap_formula_file=formula,
        tap_cask_file=cask,
        formula_resource_script=tmp_path / "generate_homebrew_formula_resources.py",
    )

    module.update_tap(
        paths=paths,
        pfm_version="0.20.16",
        pfm_sha="abcdef1234567890",
        app_version=None,
        app_sha=None,
    )

    updated = formula.read_text(encoding="utf-8")
    assert "v0.20.16/lurii_pfm-0.20.16.tar.gz" in updated
    assert 'sha256 "abcdef1234567890"' in updated


def test_sync_formula_resources_invokes_generator_write():
    module = _load_release_module()
    paths = module.RepoPaths(
        pfm_repo=Path("/repo"),
        app_repo=Path("/app"),
        tap_repo=Path("/tap"),
        pfm_version_file=Path("/repo/src/pfm/__init__.py"),
        pfm_test_file=Path("/repo/tests/test_updates.py"),
        app_project_file=Path("/app/project.pbxproj"),
        app_export_options=Path("/app/ExportOptions.plist"),
        tap_formula_file=Path("/tap/Formula/lurii-pfm.rb"),
        tap_cask_file=Path("/tap/Casks/lurii-finance.rb"),
        formula_resource_script=Path("/repo/scripts/generate_homebrew_formula_resources.py"),
    )

    with patch.object(module, "run") as mock_run:
        module.sync_formula_resources(paths)

    mock_run.assert_called_once_with(
        [
            "python3",
            "/repo/scripts/generate_homebrew_formula_resources.py",
            "--lock",
            "/repo/uv.lock",
            "--formula",
            "/tap/Formula/lurii-pfm.rb",
            "--write",
            "--check",
        ],
        cwd=Path("/repo"),
    )


def test_verify_formula_resources_invokes_generator_check():
    module = _load_release_module()
    paths = module.RepoPaths(
        pfm_repo=Path("/repo"),
        app_repo=Path("/app"),
        tap_repo=Path("/tap"),
        pfm_version_file=Path("/repo/src/pfm/__init__.py"),
        pfm_test_file=Path("/repo/tests/test_updates.py"),
        app_project_file=Path("/app/project.pbxproj"),
        app_export_options=Path("/app/ExportOptions.plist"),
        tap_formula_file=Path("/tap/Formula/lurii-pfm.rb"),
        tap_cask_file=Path("/tap/Casks/lurii-finance.rb"),
        formula_resource_script=Path("/repo/scripts/generate_homebrew_formula_resources.py"),
    )

    with patch.object(module, "run") as mock_run:
        module.verify_formula_resources(paths)

    mock_run.assert_called_once_with(
        [
            "python3",
            "/repo/scripts/generate_homebrew_formula_resources.py",
            "--lock",
            "/repo/uv.lock",
            "--formula",
            "/tap/Formula/lurii-pfm.rb",
            "--check",
        ],
        cwd=Path("/repo"),
    )
