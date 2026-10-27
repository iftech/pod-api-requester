from collections import defaultdict
from contextlib import asynccontextmanager

from fastapi import FastAPI

from routers.probe_logging import suppress_successful_probe_logs
from routers.registry import build_routers
from utils import setup_logger

logger = setup_logger(__file__)


def create_app(config) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        suppress_successful_probe_logs()
        yield

    app = FastAPI(lifespan=lifespan)

    app.state.namespace = (
        open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read().strip() or "default"
    )

    async def get_config() -> dict:
        logger.debug(f"get_config: {config}")
        return config

    app.state.cache = defaultdict(dict)

    for router in build_routers(get_config):
        app.include_router(router)

    return app
