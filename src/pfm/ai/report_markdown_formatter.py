"""Final markdown normalization for AI weekly reports."""

from __future__ import annotations

import re
from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pfm.ai.base import CommentarySection

_LIST_MARKER_RE = re.compile(r"^(?P<indent>\s*)(?P<marker>(?:[-*•])|\d+\.)\s+(?P<body>.*)$")
_INLINE_LIST_RE = re.compile(r"(?P<boundary>[.!?:;])[ \t]+(?P<marker>(?:[-*•])|\d+\.)\s+")
_INLINE_UNORDERED_RE = re.compile(r"[ \t]+(?:[-*•])\s+")
_INLINE_ORDERED_RE = re.compile(r"[ \t]+(?:\d+)\.\s+")
_MULTI_BLANK_LINES_RE = re.compile(r"\n{3,}")


def normalize_report_section_body(text: str) -> str:
    """Normalize markdown returned by the model into stable paragraph/list blocks."""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = "\n".join(line.rstrip() for line in normalized.splitlines())
    normalized = _split_inline_lists(normalized)
    blocks = _parse_blocks(normalized)
    rendered = _render_blocks(blocks)
    rendered = _MULTI_BLANK_LINES_RE.sub("\n\n", rendered)
    return rendered.strip()


def normalize_report_sections(sections: tuple[CommentarySection, ...]) -> tuple[CommentarySection, ...]:
    """Normalize all section descriptions in order."""
    return tuple(
        replace(section, description=normalize_report_section_body(section.description)) for section in sections
    )


def normalize_report_text(text: str) -> str:
    """Normalize plain report text conservatively for legacy cached rows."""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = "\n".join(line.rstrip() for line in normalized.splitlines())
    normalized = _split_inline_lists(normalized)
    normalized = _MULTI_BLANK_LINES_RE.sub("\n\n", normalized)
    return normalized.strip()


def _split_inline_lists(text: str) -> str:
    lines: list[str] = []
    for raw_line in text.split("\n"):
        line = raw_line
        while True:
            match = _INLINE_LIST_RE.search(line)
            if match is None:
                break
            prefix = line[: match.start("marker")].rstrip()
            suffix = line[match.start("marker") :].lstrip()
            line = f"{prefix}\n\n{suffix}"
        lines.append(line)
    return "\n".join(lines)


def _parse_blocks(text: str) -> list[tuple[str, list[str]]]:  # noqa: C901
    lines = text.split("\n")
    blocks: list[tuple[str, list[str]]] = []
    paragraph_lines: list[str] = []
    index = 0

    def flush_paragraph() -> None:
        nonlocal paragraph_lines
        if not paragraph_lines:
            return
        joined = " ".join(part.strip() for part in paragraph_lines if part.strip()).strip()
        if joined:
            blocks.append(("paragraph", [joined]))
        paragraph_lines = []

    while index < len(lines):
        line = lines[index].strip()
        if not line:
            flush_paragraph()
            index += 1
            continue

        marker_match = _LIST_MARKER_RE.match(line)
        if marker_match:
            flush_paragraph()
            block_type = "ordered" if marker_match.group("marker")[0].isdigit() else "unordered"
            items: list[str] = []
            while index < len(lines):
                candidate = lines[index].strip()
                next_match = _LIST_MARKER_RE.match(candidate)
                if not candidate or next_match is None:
                    break
                next_type = "ordered" if next_match.group("marker")[0].isdigit() else "unordered"
                if next_type != block_type:
                    break
                items.extend(_split_list_item_bodies(next_match.group("body").strip(), block_type))
                index += 1
            if items:
                blocks.append((block_type, items))
            continue

        paragraph_lines.append(line)
        index += 1

    flush_paragraph()
    return blocks


def _render_blocks(blocks: list[tuple[str, list[str]]]) -> str:
    merged_blocks: list[tuple[str, list[str]]] = []
    for block_type, items in blocks:
        if merged_blocks and block_type in {"unordered", "ordered"} and merged_blocks[-1][0] == block_type:
            merged_type, merged_items = merged_blocks[-1]
            merged_blocks[-1] = (merged_type, [*merged_items, *items])
            continue
        merged_blocks.append((block_type, items))

    rendered_blocks: list[str] = []
    for block_type, items in merged_blocks:
        if block_type == "paragraph":
            rendered_blocks.append(items[0])
            continue
        if block_type == "unordered":
            rendered_blocks.append("\n".join(f"- {item}" for item in items))
            continue
        rendered_blocks.append("\n".join(f"{index}. {item}" for index, item in enumerate(items, start=1)))
    return "\n\n".join(rendered_blocks)


def _split_list_item_bodies(body: str, block_type: str) -> list[str]:
    pattern = _INLINE_ORDERED_RE if block_type == "ordered" else _INLINE_UNORDERED_RE
    parts = [part.strip() for part in pattern.split(body) if part.strip()]
    return parts or [body.strip()]
