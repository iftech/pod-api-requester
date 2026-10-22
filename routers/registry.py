from typing import Iterable

from fastapi import APIRouter

from routers.generic import create_router as create_generic_router
from routers.storage import create_router as create_storage_router
from routers.waku import create_router as create_waku_router


def build_routers(get_config) -> Iterable[APIRouter]:
    yield create_generic_router(get_config)
    yield create_waku_router(get_config)
    yield create_storage_router(get_config)
