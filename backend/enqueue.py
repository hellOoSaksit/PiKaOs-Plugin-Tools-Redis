"""Job-enqueue side of the redis tool — the arq producer the web app uses to hand off background work.

Bound into the DI container under `redis.Queue` by this tool's register(). The worker (worker.py) is the
consumer; this is the producer. The arq pool is created lazily and reused. Enqueue is best-effort: a Redis
outage degrades the feature (e.g. the file is stored, just not indexed yet) rather than failing the
caller's request — same A9 fail-open spirit as the connection read path.
"""
from __future__ import annotations

import logging

from arq import create_pool
from arq.connections import ArqRedis, RedisSettings

log = logging.getLogger("pikaos.redis.queue")


class Enqueuer:
    """A reusable, lazily-connected arq enqueue handle. `enqueue(job, *args)` returns True if accepted,
    False on a Redis outage (logged) — never raises into the caller's request."""

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._pool: ArqRedis | None = None

    async def _get_pool(self) -> ArqRedis:
        if self._pool is None:
            self._pool = await create_pool(RedisSettings.from_dsn(self._redis_url))
        return self._pool

    async def enqueue(self, job: str, *args) -> bool:
        try:
            pool = await self._get_pool()
            await pool.enqueue_job(job, *args)
            return True
        except Exception as exc:  # noqa: BLE001 — best-effort: never fail the caller's request
            log.warning("could not enqueue %s: %s", job, exc)
            return False
