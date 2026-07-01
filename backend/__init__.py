"""redis — datastore Tool: provides the Redis connection + the arq job queue as DI contracts, and owns
the arq worker runtime (worker.py). Part of the zero-datastore-kernel migration: `redis` + `arq` live in
THIS tool's image, not the kernel's.

`register()` (run in both the web lifespan and the worker composition root) creates the aioredis client
and the arq enqueue handle from `settings.redis_url` and binds them:
  redis.Connection — the aioredis client (auth session store, ai run-cancel/realtime resolve it)
  redis.Queue      — a best-effort `enqueue(job, *args)` handle (knowledge ingestion resolves it)

Unbound (tool disabled) → consumers degrade: auth read-paths fail open, enqueue is a no-op. The worker
(arq app.plugins.redis.worker.WorkerSettings) is a separate module; it reuses the kernel plugin_loader
composition root, so it imports no plugin.
"""
from __future__ import annotations

import redis.asyncio as aioredis

from .enqueue import Enqueuer


def register(ctx) -> None:
    from ...core.config import settings
    from ...core.contracts import REDIS_CONNECTION, REDIS_QUEUE

    client = aioredis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    ctx.container.bind(REDIS_CONNECTION, client)
    ctx.container.bind(REDIS_QUEUE, Enqueuer(settings.redis_url))


__all__ = ["register"]
