"""Shared mutable server runtime state stored inside aiohttp app."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    import asyncio

    from pfm.db.repository import Repository
    from pfm.pricing.coingecko import PricingService
    from pfm.server.ws import EventBroadcaster


@dataclass(slots=True)
class ServerRuntimeState:
    """Mutable runtime fields that change after aiohttp app startup."""

    collecting: bool = False
    collection_task: asyncio.Task[None] | None = None
    scheduler_task: asyncio.Task[None] | None = None
    generating_commentary: bool = False
    commentary_task: asyncio.Task[None] | None = None
    bg_tasks: set[asyncio.Task[None]] = field(default_factory=set)
    db_locked: bool = False
    db_key: str | None = None
    repo: Repository | None = None
    pricing: PricingService | None = None
    broadcaster: EventBroadcaster | None = None


RUNTIME_STATE = web.AppKey("runtime_state", ServerRuntimeState)


def get_runtime_state(app: web.Application) -> ServerRuntimeState:
    """Return the mutable runtime state container for the aiohttp app."""
    state: ServerRuntimeState = app[RUNTIME_STATE]
    return state


def get_repo(app: web.Application) -> Repository:
    """Return the initialized shared repository."""
    repo = get_runtime_state(app).repo
    if repo is None:
        msg = "Repository is not initialized"
        raise RuntimeError(msg)
    return repo


def get_pricing(app: web.Application) -> PricingService:
    """Return the initialized shared pricing service."""
    pricing = get_runtime_state(app).pricing
    if pricing is None:
        msg = "Pricing service is not initialized"
        raise RuntimeError(msg)
    return pricing


def get_broadcaster(app: web.Application) -> EventBroadcaster:
    """Return the initialized WebSocket broadcaster."""
    broadcaster = get_runtime_state(app).broadcaster
    if broadcaster is None:
        msg = "Broadcaster is not initialized"
        raise RuntimeError(msg)
    return broadcaster
