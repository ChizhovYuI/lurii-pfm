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


def _find_value_quote(text: str, key: str) -> int | None:
    match = re.search(rf'"{re.escape(key)}"\s*:\s*"', text)
    if match is None:
        return None
    return match.end() - 1


def _decode_json_string_payload(payload: str) -> str:
    result: list[str] = []
    idx = 0
    length = len(payload)
    escapes = {
        '"': '"',
        "\\": "\\",
        "/": "/",
        "b": "\b",
        "f": "\f",
        "n": "\n",
        "r": "\r",
        "t": "\t",
    }
    while idx < length:
        ch = payload[idx]
        if ch != "\\":
            result.append(ch)
            idx += 1
            continue
        idx += 1
        if idx >= length:
            break
        esc = payload[idx]
        if esc == "u":
            if idx + 4 >= length:
                break
            codepoint = payload[idx + 1 : idx + 5]
            if not re.fullmatch(r"[0-9a-fA-F]{4}", codepoint):
                break
            result.append(chr(int(codepoint, 16)))
            idx += 5
            continue
        result.append(escapes.get(esc, esc))
        idx += 1
    return "".join(result)


def _parse_json_string_at(text: str, quote_idx: int, *, allow_unterminated: bool) -> tuple[str, int] | None:
    if quote_idx >= len(text) or text[quote_idx] != '"':
        return None
    idx = quote_idx + 1
    payload: list[str] = []
    escape_next = False
    while idx < len(text):
        ch = text[idx]
        if escape_next:
            payload.append(ch)
            escape_next = False
            idx += 1
            continue
        if ch == "\\":
            payload.append(ch)
            escape_next = True
            idx += 1
            continue
        if ch == '"':
            return _decode_json_string_payload("".join(payload)), idx + 1
        payload.append(ch)
        idx += 1
    if allow_unterminated:
        return _decode_json_string_payload("".join(payload)).strip(), idx
    return None


def _recover_partial_section(text: str) -> CommentarySection | None:
    title_quote_idx = _find_value_quote(text, "title")
    description_quote_idx = _find_value_quote(text, "description")
    if title_quote_idx is None or description_quote_idx is None:
        return None

    title_result = _parse_json_string_at(text, title_quote_idx, allow_unterminated=False)
    description_result = _parse_json_string_at(text, description_quote_idx, allow_unterminated=True)
    if title_result is None or description_result is None:
        return None

    title, _ = title_result
    description, _ = description_result
    normalized_title = title.strip()
    normalized_description = description.strip()
    if not normalized_title or not normalized_description:
        return None
    return CommentarySection(title=normalized_title, description=normalized_description)


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
            partial = _recover_partial_section(source[idx:])
            if partial is not None:
                sections.append(partial)
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
