"""Tests for Homebrew formula resource generation."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_generator_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts/generate_homebrew_formula_resources.py"
    spec = importlib.util.spec_from_file_location("generate_homebrew_formula_resources", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_runtime_dependency_closure_includes_mcp():
    module = _load_generator_module()
    lock_data = module.load_lock(Path(__file__).resolve().parents[1] / "uv.lock")
    indexed = module.package_index(lock_data)

    closure = module.compute_runtime_closure(indexed)

    assert "mcp" in closure
    assert "pywin32" not in closure


def test_generated_resources_include_mcp_transitive_dependencies():
    module = _load_generator_module()
    lock_data = module.load_lock(Path(__file__).resolve().parents[1] / "uv.lock")

    resources = module.build_resources(lock_data)
    names = {resource.name for resource in resources}

    assert "mcp" in names
    assert "httpx-sse" in names
    assert "jsonschema" in names
    assert "jsonschema-specifications" in names
    assert "python-multipart" in names
    assert "sse-starlette" in names
    assert "starlette" in names
    assert "uvicorn" in names
    assert "referencing" in names
    assert "rpds-py" in names


def test_select_wheel_prefers_macos_arm64_wheels_when_available():
    module = _load_generator_module()
    lock_data = module.load_lock(Path(__file__).resolve().parents[1] / "uv.lock")
    indexed = module.package_index(lock_data)

    rpds_url, _ = module.select_wheel(indexed["rpds-py"])

    assert "macosx_11_0_arm64" in rpds_url


def test_select_wheel_prefers_abi3_universal_when_no_cp313_wheel_exists():
    module = _load_generator_module()
    lock_data = module.load_lock(Path(__file__).resolve().parents[1] / "uv.lock")
    indexed = module.package_index(lock_data)

    pynacl_url, _ = module.select_wheel(indexed["pynacl"])

    assert "cp38-abi3" in pynacl_url
    assert "macosx_10_10_universal2" in pynacl_url


def test_generated_output_is_deterministic():
    module = _load_generator_module()
    lock_data = module.load_lock(Path(__file__).resolve().parents[1] / "uv.lock")

    first = module.render_resources(module.build_resources(lock_data))
    second = module.render_resources(module.build_resources(lock_data))

    assert first == second


def test_sync_formula_resources_updates_auto_generated_region(tmp_path):
    module = _load_generator_module()
    formula_path = tmp_path / "lurii-pfm.rb"
    formula_path.write_text(
        """class LuriiPfm < Formula
  # BEGIN AUTO-GENERATED RESOURCES
  old resource
  # END AUTO-GENERATED RESOURCES
end
""",
        encoding="utf-8",
    )

    module.sync_formula_resources(
        formula_path=formula_path,
        lock_path=Path(__file__).resolve().parents[1] / "uv.lock",
        write=True,
    )

    updated = formula_path.read_text(encoding="utf-8")
    assert 'resource "mcp" do' in updated
    assert "old resource" not in updated


def test_check_formula_resources_detects_stale_region(tmp_path):
    module = _load_generator_module()
    formula_path = tmp_path / "lurii-pfm.rb"
    formula_path.write_text(
        """class LuriiPfm < Formula
  # BEGIN AUTO-GENERATED RESOURCES
  stale resource
  # END AUTO-GENERATED RESOURCES
end
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="out of sync"):
        module.check_formula_resources(
            formula_path=formula_path,
            lock_path=Path(__file__).resolve().parents[1] / "uv.lock",
        )
