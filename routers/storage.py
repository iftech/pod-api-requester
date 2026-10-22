"""Streaming Logos Storage operations; experiment scheduling belongs to the caller."""

import asyncio
import hashlib
import json
import os
import time
from contextlib import asynccontextmanager
from typing import Annotated, Awaitable, Callable, Literal, Union
from urllib.parse import quote

import aiohttp
from fastapi import APIRouter, Depends, HTTPException, Request
from kubernetes.client.exceptions import ApiException
from pydantic import BaseModel, Field, PositiveInt, field_validator
from urllib3.exceptions import HTTPError

from common import get_pod_infos
from routers.deps import TargetConfig, TargetName, unwrap_arg
from schemas import NotFoundError

CHUNK_SIZE = 64 * 1024
MAX_CID_BYTES = 2048
MAX_ERROR_BYTES = 4096
MAX_CONCURRENT_OPERATIONS = 8


class StorageRequest(BaseModel):
    target: Annotated[Union[TargetName, TargetConfig], Field(discriminator="kind")]
    timeout_seconds: float = Field(default=60, gt=0, allow_inf_nan=False)


class UploadRequest(StorageRequest):
    file_size_bytes: PositiveInt = 1024 * 1024


class DownloadRequest(StorageRequest):
    cid: str = Field(min_length=1, max_length=MAX_CID_BYTES)
    expected_sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    expected_size_bytes: PositiveInt
    transport: Literal["direct", "mix"] = "direct"

    @field_validator("cid")
    @classmethod
    def strip_cid(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("CID cannot be blank")
        return value

    @field_validator("expected_sha256")
    @classmethod
    def normalize_hash(cls, value):
        return value.lower()


async def _resolve_target(data, request, config):
    target = unwrap_arg(data.target, "targets", config)
    if not 1 <= target.port <= 65535:
        raise HTTPException(status_code=422, detail="Target port must be between 1 and 65535")
    pods = await asyncio.to_thread(
        get_pod_infos,
        targets=[target],
        namespace=request.app.state.namespace,
        cache=request.app.state.cache,
    )
    if not pods:
        raise HTTPException(status_code=404, detail="No pod matches the target")
    if len(pods) != 1:
        raise HTTPException(
            status_code=409,
            detail="Storage operations require exactly one pod; narrow the target filters",
        )
    pod = pods[0]
    pod_ip = pod.pod.status.pod_ip if pod.pod.status else None
    if not pod_ip:
        raise HTTPException(status_code=503, detail=f"Pod {pod.pod_name} has no IP")
    host = f"[{pod_ip}]" if ":" in pod_ip else pod_ip
    return pod.pod_name, f"http://{host}:{target.port}/api/storage/v1/data"


async def _check_status(response):
    if response.status != 200:
        # Bound diagnostic bodies too: the node may return arbitrary content.
        body = await response.content.read(MAX_ERROR_BYTES)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Storage node returned an error",
                "upstream_status_code": response.status,
                "body": body.decode("utf-8", errors="replace"),
            },
        )


async def _upload(session, url, data):
    digest = hashlib.sha256()
    generated = 0

    async def chunks():
        nonlocal generated
        while generated < data.file_size_bytes:
            chunk = os.urandom(min(CHUNK_SIZE, data.file_size_bytes - generated))
            digest.update(chunk)
            generated += len(chunk)
            yield chunk

    started = time.perf_counter()
    async with session.post(
        url,
        data=chunks(),
        headers={
            "Content-Type": "application/octet-stream",
            "Content-Length": str(data.file_size_bytes),
        },
        allow_redirects=False,
    ) as response:
        await _check_status(response)
        cid_bytes = bytearray()
        async for chunk in response.content.iter_chunked(MAX_CID_BYTES + 1):
            cid_bytes.extend(chunk)
            if len(cid_bytes) > MAX_CID_BYTES:
                raise HTTPException(status_code=502, detail="Storage node returned an oversized CID")
        try:
            cid = cid_bytes.decode("ascii").strip()
        except UnicodeDecodeError as exc:
            raise HTTPException(status_code=502, detail="Storage node returned an invalid CID") from exc
        if not cid or generated != data.file_size_bytes:
            raise HTTPException(status_code=502, detail="Storage upload did not complete")
    return {
        "cid": cid,
        "sha256": digest.hexdigest(),
        "file_size_bytes": generated,
        "seconds": time.perf_counter() - started,
    }


async def _download(session, url, data):
    digest = hashlib.sha256()
    size = 0
    started = time.perf_counter()
    async with session.get(
        url, params={"transport": data.transport}, allow_redirects=False
    ) as response:
        await _check_status(response)
        async for chunk in response.content.iter_chunked(CHUNK_SIZE):
            digest.update(chunk)
            size += len(chunk)
            if size > data.expected_size_bytes:
                raise HTTPException(
                    status_code=409, detail="Downloaded data exceeds the expected size"
                )
    actual_hash = digest.hexdigest()
    if size != data.expected_size_bytes or actual_hash != data.expected_sha256:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "Downloaded data failed verification",
                "expected_size_bytes": data.expected_size_bytes,
                "file_size_bytes": size,
                "expected_sha256": data.expected_sha256,
                "sha256": actual_hash,
            },
        )
    return {
        "cid": data.cid,
        "sha256": actual_hash,
        "file_size_bytes": size,
        "seconds": time.perf_counter() - started,
        "verified": True,
    }


async def _run_operation(operation, request, data, config):
    try:
        # Includes capacity wait, pod lookup, and the complete HTTP exchange.
        async with asyncio.timeout(data.timeout_seconds):
            async with request.app.state.storage_semaphore:
                pod_name, url = await _resolve_target(data, request, config)
                if operation == "download":
                    url += f"/{quote(data.cid, safe='')}/network/stream"
                handler = _upload if operation == "upload" else _download
                receipt = await handler(request.app.state.storage_session, url, data)
                return {
                    "request": {"operation": operation, "pod": pod_name, "url": url},
                    "response": {"status_code": 200, "text": json.dumps(receipt)},
                }
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=f"Storage {operation} timed out") from exc
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ApiException, HTTPError) as exc:
        raise HTTPException(status_code=503, detail="Kubernetes pod lookup failed") from exc
    except aiohttp.ClientError as exc:
        raise HTTPException(status_code=502, detail=f"Storage {operation} connection failed") from exc


def create_router(
    get_config: Callable[[], Awaitable[dict]],
    *,
    max_concurrent_operations: int = MAX_CONCURRENT_OPERATIONS,
) -> APIRouter:
    @asynccontextmanager
    async def lifespan(app):
        app.state.storage_semaphore = asyncio.Semaphore(max_concurrent_operations)
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=max_concurrent_operations),
            timeout=aiohttp.ClientTimeout(total=None, connect=10),
            headers={"Accept-Encoding": "identity"},
            auto_decompress=False,
        ) as session:
            app.state.storage_session = session
            yield

    router = APIRouter(prefix="/storage", tags=["storage"], lifespan=lifespan)

    @router.post("/upload")
    async def upload(request: Request, data: UploadRequest, config=Depends(get_config)):
        return await _run_operation("upload", request, data, config)

    @router.post("/download")
    async def download(request: Request, data: DownloadRequest, config=Depends(get_config)):
        return await _run_operation("download", request, data, config)

    return router
