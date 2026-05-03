#!/usr/bin/env python3
# ruff: noqa: T201, TRY004
"""Mint a throwaway bunq sandbox API key.

Calls `/v1/sandbox-user-person` on the public sandbox host. The endpoint
is unauthenticated and returns an `ApiKey` payload bound to a freshly
provisioned sandbox UserPerson. Use the printed key with
`pfm source add` (select environment=sandbox).

The bunq sandbox is rate-limited; treat any minted key as ephemeral and
do not commit it to git.

Usage:
    python scripts/bunq_sandbox_key.py
"""

from __future__ import annotations

import sys

import httpx

_SANDBOX_URL = "https://public-api.sandbox.bunq.com/v1/sandbox-user-person"


def _extract_api_key(payload: dict[str, object]) -> str:
    response = payload.get("Response")
    if not isinstance(response, list):
        msg = f"unexpected sandbox payload shape: {payload!r}"
        raise RuntimeError(msg)
    for item in response:
        if not isinstance(item, dict):
            continue
        api_key = item.get("ApiKey")
        if isinstance(api_key, dict) and isinstance(api_key.get("api_key"), str):
            return api_key["api_key"]
    msg = f"sandbox response missing ApiKey: {payload!r}"
    raise RuntimeError(msg)


def main() -> int:
    headers = {
        "User-Agent": "lurii-pfm-sandbox/1",
        "Cache-Control": "no-cache",
    }
    try:
        resp = httpx.post(_SANDBOX_URL, headers=headers, timeout=30.0)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"sandbox request failed: {exc}", file=sys.stderr)
        return 1

    api_key = _extract_api_key(resp.json())
    print(api_key)
    print(
        "Add to pfm with:  pfm source add  -> select bunq, paste this key, set environment=sandbox.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
