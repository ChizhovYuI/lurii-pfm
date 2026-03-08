#!/usr/bin/env python3
# ruff: noqa: T201, TRY003, EM101, EM102, TRY004, C901, PLR0912, PLR2004, SIM108
"""Generate Homebrew resource blocks for the lurii-pfm formula from uv.lock."""

from __future__ import annotations

import argparse
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

AUTO_GENERATED_BEGIN = "  # BEGIN AUTO-GENERATED RESOURCES"
AUTO_GENERATED_END = "  # END AUTO-GENERATED RESOURCES"
ROOT_PACKAGE = "lurii-pfm"
PYTHON_TAG = "cp313"
REQUIRED_MCP_RESOURCES = (
    "mcp",
    "httpx-sse",
    "jsonschema",
    "jsonschema-specifications",
    "python-multipart",
    "sse-starlette",
    "starlette",
    "uvicorn",
)


@dataclass(frozen=True)
class Resource:
    name: str
    url: str
    sha256: str
    pure_python: bool


def load_lock(lock_path: Path) -> dict[str, object]:
    return tomllib.loads(lock_path.read_text(encoding="utf-8"))


def package_index(lock_data: dict[str, object]) -> dict[str, dict[str, object]]:
    packages = lock_data.get("package")
    if not isinstance(packages, list):
        raise ValueError("uv.lock is missing the package list.")
    indexed: dict[str, dict[str, object]] = {}
    for package in packages:
        if not isinstance(package, dict):
            continue
        name = package.get("name")
        if isinstance(name, str):
            indexed[name] = package
    if ROOT_PACKAGE not in indexed:
        raise ValueError(f"uv.lock does not contain the root package {ROOT_PACKAGE!r}.")
    return indexed


def marker_applies(marker: str | None) -> bool:
    if marker is None:
        return True
    normalized = " ".join(marker.split())
    if normalized == "sys_platform == 'win32'":
        return False
    if normalized == "sys_platform != 'emscripten'":
        return True
    if normalized == "implementation_name != 'PyPy'":
        return True
    if normalized == "platform_python_implementation != 'PyPy'":
        return True
    if normalized == (
        "platform_machine == 'AMD64' or platform_machine == 'WIN32' or "
        "platform_machine == 'aarch64' or platform_machine == 'amd64' or "
        "platform_machine == 'ppc64le' or platform_machine == 'win32' or "
        "platform_machine == 'x86_64'"
    ):
        return False
    raise ValueError(f"Unsupported dependency marker in uv.lock: {marker}")


def _dependency_name(dep: object) -> str | None:
    if not isinstance(dep, dict):
        return None
    name = dep.get("name")
    return name if isinstance(name, str) else None


def dependency_extras(dep: dict[str, object]) -> set[str]:
    raw_extras = dep.get("extra")
    if not isinstance(raw_extras, list):
        return set()
    return {extra for extra in raw_extras if isinstance(extra, str)}


def dependency_marker(dep: dict[str, object]) -> str | None:
    marker = dep.get("marker")
    return marker if isinstance(marker, str) else None


def package_dependencies(package: dict[str, object]) -> list[dict[str, object]]:
    dependencies = package.get("dependencies")
    if not isinstance(dependencies, list):
        return []
    return [dep for dep in dependencies if isinstance(dep, dict)]


def compute_runtime_closure(indexed_packages: dict[str, dict[str, object]]) -> list[str]:
    pending: list[str] = [ROOT_PACKAGE]
    requested_extras: dict[str, set[str]] = {}
    seen: set[str] = set()

    while pending:
        package_name = pending.pop()
        package = indexed_packages[package_name]
        seen.add(package_name)

        for dep in package_dependencies(package):
            if not marker_applies(dependency_marker(dep)):
                continue
            dep_name = _dependency_name(dep)
            if dep_name is None:
                continue
            extras = dependency_extras(dep)
            existing_extras = requested_extras.setdefault(dep_name, set())
            extras_changed = not extras.issubset(existing_extras)
            if extras_changed:
                existing_extras.update(extras)
            if dep_name not in seen or extras_changed:
                pending.append(dep_name)

        for extra in requested_extras.get(package_name, set()):
            optional = package.get("optional-dependencies", {})
            if not isinstance(optional, dict):
                continue
            optional_dependencies = optional.get(extra)
            if not isinstance(optional_dependencies, list):
                continue
            for dep in optional_dependencies:
                if not isinstance(dep, dict):
                    continue
                if not marker_applies(dependency_marker(dep)):
                    continue
                dep_name = _dependency_name(dep)
                if dep_name is None:
                    continue
                extras = dependency_extras(dep)
                existing_extras = requested_extras.setdefault(dep_name, set())
                extras_changed = not extras.issubset(existing_extras)
                if extras_changed:
                    existing_extras.update(extras)
                if dep_name not in seen or extras_changed:
                    pending.append(dep_name)

    return sorted(name for name in seen if name != ROOT_PACKAGE)


def is_pure_wheel(filename: str) -> bool:
    return filename.endswith(("py3-none-any.whl", "py2.py3-none-any.whl"))


def _split_wheel_tags(filename: str) -> tuple[str, str, str]:
    stem = filename.removesuffix(".whl")
    parts = stem.rsplit("-", 3)
    if len(parts) != 4:
        raise ValueError(f"Unexpected wheel filename: {filename}")
    return parts[1], parts[2], parts[3]


def python_tag_priority(py_tag: str, abi_tag: str) -> int | None:
    python_tags = py_tag.split(".")
    if "py3" in python_tags and abi_tag == "none":
        return 30
    if "py2.py3" in python_tags and abi_tag == "none":
        return 31
    if PYTHON_TAG in python_tags and abi_tag == PYTHON_TAG:
        return 0
    if PYTHON_TAG in python_tags and abi_tag == "abi3":
        return 10
    for tag in python_tags:
        if re.fullmatch(r"cp\d+", tag) and abi_tag == "abi3":
            raw = tag.removeprefix("cp")
            if len(raw) == 2:
                version = int(raw[0]) * 100 + int(raw[1:])
            else:
                version = int(raw)
            if 300 <= version <= 313:
                return 20 + (313 - version)
    return None


def platform_priority(platform_tag: str) -> int | None:
    platforms = platform_tag.split(".")
    if any("macosx" in platform and "arm64" in platform for platform in platforms):
        return 0
    if any("macosx" in platform and "universal2" in platform for platform in platforms):
        return 1
    if any("macosx" in platform for platform in platforms):
        return 2
    return None


def wheel_priority(filename: str) -> tuple[int, int, str] | None:
    py_tag, abi_tag, platform_tag = _split_wheel_tags(filename)
    if platform_tag == "any":
        if python_tag_priority(py_tag, abi_tag) is not None:
            return (99, 0, filename)
        return None
    python_priority = python_tag_priority(py_tag, abi_tag)
    if python_priority is None:
        return None
    platform = platform_priority(platform_tag)
    if platform is None:
        return None
    return (platform, python_priority, filename)


def select_wheel(package: dict[str, object]) -> tuple[str, str]:
    wheels = package.get("wheels")
    if not isinstance(wheels, list) or not wheels:
        raise ValueError(f"Package {package['name']} does not have any wheels in uv.lock.")

    candidates: list[tuple[tuple[int, int, str], dict[str, object]]] = []
    for wheel in wheels:
        if not isinstance(wheel, dict):
            continue
        url = wheel.get("url")
        digest = wheel.get("hash")
        if not isinstance(url, str) or not isinstance(digest, str):
            continue
        filename = url.rsplit("/", 1)[-1]
        priority = wheel_priority(filename)
        if priority is None:
            continue
        candidates.append((priority, wheel))

    if not candidates:
        raise ValueError(f"Package {package['name']} has no macOS-arm64-compatible wheel in uv.lock.")

    _, best = min(candidates, key=lambda item: item[0])
    url = best["url"]
    digest = best["hash"]
    if not isinstance(url, str) or not isinstance(digest, str):
        raise ValueError(f"Package {package['name']} has an invalid wheel record.")
    return url, digest.removeprefix("sha256:")


def build_resources(lock_data: dict[str, object]) -> list[Resource]:
    indexed = package_index(lock_data)
    closure = compute_runtime_closure(indexed)
    resources: list[Resource] = []
    for name in closure:
        package = indexed[name]
        url, sha256 = select_wheel(package)
        filename = url.rsplit("/", 1)[-1]
        resources.append(
            Resource(
                name=name,
                url=url,
                sha256=sha256,
                pure_python=is_pure_wheel(filename),
            )
        )
    return sorted(resources, key=lambda resource: (resource.pure_python, resource.name.lower()))


def render_resources(resources: list[Resource]) -> str:
    native = [resource for resource in resources if not resource.pure_python]
    pure = [resource for resource in resources if resource.pure_python]
    lines = [
        "  # --- All dependencies as pre-built wheels (no compilation needed) ---",
        "",
    ]
    if native:
        lines.append("  # Native extensions (platform-specific wheels)")
        for resource in native:
            lines.extend(render_resource_block(resource))
    if pure:
        if native:
            lines.append("")
        lines.append("  # Pure-Python wheels")
        for resource in pure:
            lines.extend(render_resource_block(resource))
    return "\n".join(lines)


def render_resource_block(resource: Resource) -> list[str]:
    return [
        f'  resource "{resource.name}" do',
        f'    url "{resource.url}"',
        f'    sha256 "{resource.sha256}"',
        "  end",
        "",
    ]


def apply_resource_region(formula_text: str, rendered_resources: str) -> str:
    pattern = re.compile(
        rf"(?P<before>{re.escape(AUTO_GENERATED_BEGIN)}\n)(?P<body>.*?)(?P<after>\n{re.escape(AUTO_GENERATED_END)})",
        flags=re.DOTALL,
    )
    match = pattern.search(formula_text)
    if match is None:
        raise ValueError("Formula is missing auto-generated resource markers.")
    return formula_text[: match.start("body")] + rendered_resources + formula_text[match.start("after") :]


def check_formula_resources(*, formula_path: Path, lock_path: Path) -> None:
    formula_text = formula_path.read_text(encoding="utf-8")
    expected = apply_resource_region(formula_text, render_resources(build_resources(load_lock(lock_path))))
    if formula_text != expected:
        raise ValueError(f"Formula resources in {formula_path} are out of sync with {lock_path}.")
    verify_formula_contains_required_mcp_resources(formula_path)


def sync_formula_resources(*, formula_path: Path, lock_path: Path, write: bool) -> str:
    lock_data = load_lock(lock_path)
    rendered = render_resources(build_resources(lock_data))
    if not write:
        return rendered

    formula_text = formula_path.read_text(encoding="utf-8")
    updated = apply_resource_region(formula_text, rendered)
    formula_path.write_text(updated, encoding="utf-8")
    return rendered


def verify_formula_contains_required_mcp_resources(formula_path: Path) -> None:
    contents = formula_path.read_text(encoding="utf-8")
    for package in REQUIRED_MCP_RESOURCES:
        if f'resource "{package}" do' not in contents:
            raise ValueError(f"Formula is missing required MCP resource {package!r}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, default=Path("uv.lock"), help="Path to uv.lock.")
    parser.add_argument("--formula", type=Path, help="Path to Formula/lurii-pfm.rb.")
    parser.add_argument("--write", action="store_true", help="Write the generated resources into the formula.")
    parser.add_argument("--check", action="store_true", help="Verify the formula contains the expected resource block.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.write and args.formula is None:
        raise SystemExit("--write requires --formula.")
    if args.check and args.formula is None:
        raise SystemExit("--check requires --formula.")

    if args.write and args.formula is not None:
        sync_formula_resources(formula_path=args.formula, lock_path=args.lock, write=True)
        print(f"Synced Homebrew resources in {args.formula}")
    elif args.check and args.formula is not None:
        check_formula_resources(formula_path=args.formula, lock_path=args.lock)
        print(f"Validated Homebrew resources in {args.formula}")
    else:
        print(sync_formula_resources(formula_path=args.formula or Path(), lock_path=args.lock, write=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
