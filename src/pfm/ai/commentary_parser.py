"""Helpers for parsing AI commentary sections from LLM output."""

from __future__ import annotations

import json
import logging
import re

from pfm.ai.base import CommentarySection

logger = logging.getLogger(__name__)


def finalize_commentary_text(text: str) -> str:
    """Normalize line endings, strip ``<think>`` blocks, and trim whitespace."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    return text.strip()


def escape_newlines_in_json_strings(text: str) -> str:
    """Escape literal newlines inside JSON string values."""
    result: list[str] = []
    in_string = False
    escape_next = False
    for ch in text:
        if escape_next:
            result.append(ch)
            escape_next = False
            continue
        if ch == "\\" and in_string:
            result.append(ch)
            escape_next = True
            continue
        if ch == '"':
            in_string = not in_string
            result.append(ch)
            continue
        if ch == "\n" and in_string:
            result.append("\\n")
            continue
        result.append(ch)
    return "".join(result)


def parse_commentary_sections(text: str) -> tuple[CommentarySection, ...]:
    """Parse commentary as a JSON array, salvaging complete items when truncated."""
    prepared = _prepare_array_text(text)
    if prepared is None:
        logger.debug("AI response does not contain a JSON array; treating as plain text.")
        return ()

    parsed = _try_json_loads(prepared.full_array)
    if parsed is not None:
        return _coerce_sections(parsed)

    recovered = _recover_sections_from_array_source(prepared.recovery_source)
    if recovered:
        return recovered

    logger.debug("AI response is not valid JSON; treating as plain text.")
    return ()


def recover_commentary_sections(text: str) -> tuple[CommentarySection, ...]:
    """Recover complete section objects from a truncated JSON array."""
    prepared = _prepare_array_text(text)
    if prepared is None:
        return ()
    return _recover_sections_from_array_source(prepared.recovery_source)


class _PreparedArrayText:
    def __init__(self, full_array: str, recovery_source: str) -> None:
        self.full_array = full_array
        self.recovery_source = recovery_source


def _strip_code_fences(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.split("\n")
    body_lines = [line for line in lines[1:] if not line.strip().startswith("```")]
    return "\n".join(body_lines).strip()


def _prepare_array_text(text: str) -> _PreparedArrayText | None:
    finalized = finalize_commentary_text(text)
    stripped = _strip_code_fences(finalized)
    start = stripped.find("[")
    if start == -1:
        return None

    recovery_source = stripped[start:]
    end = stripped.rfind("]")
    full_array = stripped[start : end + 1] if end > start else recovery_source
    return _PreparedArrayText(full_array=full_array, recovery_source=recovery_source)


def _try_json_loads(text: str) -> list[object] | None:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = json.loads(escape_newlines_in_json_strings(text))
        except json.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, list) else None


def _coerce_section(item: object) -> CommentarySection | None:
    if not isinstance(item, dict):
        return None
    title = item.get("title")
    description = item.get("description")
    if not isinstance(title, str) or not isinstance(description, str):
        return None
    normalized_title = title.strip()
    normalized_description = description.strip()
    if not normalized_title or not normalized_description:
        return None
    return CommentarySection(title=normalized_title, description=normalized_description)


def _coerce_sections(parsed: list[object]) -> tuple[CommentarySection, ...]:
    sections: list[CommentarySection] = []
    for item in parsed:
        section = _coerce_section(item)
        if section is not None:
            sections.append(section)
    return tuple(sections)


def _skip_whitespace(text: str, idx: int) -> int:
    length = len(text)
    while idx < length and text[idx].isspace():
        idx += 1
    return idx


def _consume_next_section(
    decoder: json.JSONDecoder,
    source: str,
    idx: int,
) -> tuple[CommentarySection | None, int] | None:
    try:
        item, next_idx = decoder.raw_decode(source, idx)
    except json.JSONDecodeError:
        return None
    return _coerce_section(item), next_idx


def _recover_sections_from_array_source(text: str) -> tuple[CommentarySection, ...]:
    source = escape_newlines_in_json_strings(text)
    decoder = json.JSONDecoder()
    sections: list[CommentarySection] = []

    idx = _skip_whitespace(source, 0)
    length = len(source)
    if idx >= length or source[idx] != "[":
        return ()
    idx += 1

    while idx < length:
        idx = _skip_whitespace(source, idx)
        if idx >= length or source[idx] == "]":
            break

        parsed = _consume_next_section(decoder, source, idx)
        if parsed is None:
            break
        section, idx = parsed
        if section is not None:
            sections.append(section)
        idx = _skip_whitespace(source, idx)
        if idx >= length:
            break
        if source[idx] == ",":
            idx += 1
            continue
        if source[idx] == "]":
            break
        break

    return tuple(sections)
