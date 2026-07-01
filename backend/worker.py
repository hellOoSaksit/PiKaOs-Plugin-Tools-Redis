"""arq worker entrypoint — runs whatever jobs the enabled plugins contribute, out-of-process from the
FastAPI web app (a crashed or slow job can't take the API down). Same image, different command
(`arq app.plugins.redis.worker.WorkerSettings`); see the `worker` service in deploy/docker-compose.ai.yml.

Lives in the **redis tool** because arq is a Redis construct — the zero-datastore kernel ships neither
`redis` nor `arq`. It imports no OTHER plugin: every job (e.g. the AI plugin's `agent_run`, the knowledge
plugin's `ingest_document`) is contributed by its plugin's `jobs` list and discovered through the kernel
Loader (dynamic import). At startup the worker assembles the DI container + Event Bus and runs each enabled
plugin's `register()/boot()` in dependency order — so a plugin's `boot()` (e.g. the AI engine wiring its
runtime) happens here, and THIS tool's own register() binds `redis.Connection`/`redis.Queue` first.
"""
from __future__ import annotations

import logging

from arq.connections import RedisSettings

from ... import modules, plugin_loader
from ...core.config import settings
from ...core.container import Container
from ...core.db import SessionLocal
from ...core.events import EventBus
from ...core.logging_ctx import configure_worker_logging

log = logging.getLogger("pikaos.worker")


async def ping(ctx) -> str:
    """Trivial job — confirms the worker is wired to Redis."""
    return "pong"


async def startup(ctx) -> None:
    """Wire structured logging (B7) and assemble the plugin tier once per worker. The DI container +
    Event Bus are built here (this worker is a composition root); each enabled plugin's register()/boot()
    runs in dependency order — including this redis tool's register() (binds redis.Connection/Queue) and
    a plugin's boot() (e.g. the AI engine wiring its runtime), never importing a plugin from this module."""
    configure_worker_logging()

    container, bus = Container(), EventBus()
    ctx["container"], ctx["bus"] = container, bus  # jobs read these off the arq context
    enabled = modules.enabled_optional_modules()
    result = plugin_loader.register_plugins(enabled, modules.PLUGIN_MANIFESTS,
                                            plugin_loader.PluginContext(container=container, events=bus,
                                                                        session_factory=SessionLocal,
                                                                        settings=settings))
    if result.degraded:  # §8 — a plugin whose register/boot raised; the worker stays up
        log.warning("plugins degraded (lifecycle failed, others unaffected): %s", result.degraded)
    log.info("pikaos worker up — structured logging on · plugins booted: %s · degraded: %s",
             result.booted or "(none)", result.degraded or "(none)")


async def shutdown(ctx) -> None:
    """Tear the plugin tier down in reverse dependency order (§10) when the worker stops — fault-isolated,
    so a misbehaving shutdown() never blocks the rest. Rebuilds the PluginContext from the container + bus
    stashed on the arq context at startup."""
    container, bus = ctx.get("container"), ctx.get("bus")
    if container is None:  # startup never completed — nothing to tear down
        return
    enabled = modules.enabled_optional_modules()
    errors = plugin_loader.shutdown_plugins(enabled, modules.PLUGIN_MANIFESTS,
                                            plugin_loader.PluginContext(container=container, events=bus,
                                                                        session_factory=SessionLocal,
                                                                        settings=settings))
    if errors:
        log.warning("plugin shutdown errors (ignored): %s", errors)


def _active_functions() -> list:
    """The arq job set for this build: infra `ping` + the jobs every enabled plugin contributes via its
    `jobs` list (collected through the Loader — no plugin import here, §5). The AI plugin contributes
    `agent_run` (with its `keep_result` wrapper); knowledge contributes `ingest_document`; etc."""
    plugin_jobs = plugin_loader.collect_jobs(modules.enabled_optional_modules(), modules.PLUGIN_MANIFESTS)
    return [ping, *plugin_jobs]


class WorkerSettings:
    """arq worker config. Discovered via `arq app.plugins.redis.worker.WorkerSettings`."""

    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    on_startup = startup
    on_shutdown = shutdown
    functions = _active_functions()
