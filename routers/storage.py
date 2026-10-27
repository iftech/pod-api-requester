"""Logos Storage uploads and control requests to per-pod stream verifiers."""

import asyncio
import hashlib
import json
import os
import time
from contextlib import asynccontextmanager
from typing import Annotated, Awaitable, Callable, Union

import aiohttp
from fastapi import APIRouter, Depends, HTTPException, Request
from kubernetes.client.exceptions import ApiException
from pydantic import BaseModel, Field, PositiveInt
from urllib3.exceptions import HTTPError

from common import get_pod_infos
from routers.deps import TargetConfig, TargetName, unwrap_arg
from routers.storage_verifier import (
    CHUNK_SIZE,
    MAX_CID_BYTES,
    MAX_ERROR_BYTES,
    VERIFIER_PORT,
    DownloadReceipt,
    DownloadSpec,
    check_status as _check_status,
)
from schemas import NotFoundError

MAX_JSON_BYTES = 64 * 1024
MAX_CONCURRENT_OPERATIONS = 8
DEFAULT_UPLOAD_SIZE_BYTES = 1024 * 1024
MAX_UPLOAD_SIZE_BYTES = int(os.environ.get("STORAGE_MAX_UPLOAD_SIZE_BYTES", 100 * 1024 * 1024))
if MAX_UPLOAD_SIZE_BYTES < DEFAULT_UPLOAD_SIZE_BYTES:
    raise ValueError("STORAGE_MAX_UPLOAD_SIZE_BYTES must be at least 1048576 (1 MiB)")

StorageTarget = Annotated[Union[TargetName, TargetConfig], Field(discriminator="kind")]


class StorageRequest(BaseModel):
    target: StorageTarget
    timeout_seconds: float = Field(default=600, gt=0, allow_inf_nan=False)


class UploadRequest(StorageRequest):
    file_size_bytes: PositiveInt = Field(default=DEFAULT_UPLOAD_SIZE_BYTES, le=MAX_UPLOAD_SIZE_BYTES)


class DownloadRequest(DownloadSpec):
    target: StorageTarget
    verifier_port: int = Field(default=VERIFIER_PORT, ge=1, le=65535)


async def _resolve_target(data, request, config, *, port=None, path="/api/storage/v1/data"):
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
    node_address = (
        pod.node_address if pod.pod is None or pod.pod.status is not None else None
    )
    if not node_address:
        raise HTTPException(status_code=503, detail=f"Target {pod.pod_name} has no address")
    host = f"[{node_address}]" if ":" in node_address else node_address
    return pod.pod_name, f"http://{host}:{port if port is not None else target.port}{path}"


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


async def _read_json(response):
    body = bytearray()
    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
        body.extend(chunk)
        if len(body) > MAX_JSON_BYTES:
            raise HTTPException(status_code=502, detail="Verifier returned oversized JSON")
    if response.status != 200:
        # Preserve Storage's original status across the verifier hop for retry policy.
        try:
            error = json.loads(body)
        except ValueError:
            error = None
        detail = error.get("detail") if isinstance(error, dict) else None
        if (
            response.status == 502
            and isinstance(detail, dict)
            and type(detail.get("upstream_status_code")) is int
        ):
            raise HTTPException(status_code=502, detail=detail)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Verifier returned an error",
                "upstream_status_code": response.status,
                "body": body[:MAX_ERROR_BYTES].decode("utf-8", errors="replace"),
            },
        )
    try:
        result = json.loads(body)
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Verifier returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise HTTPException(status_code=502, detail="Verifier returned a non-object JSON value")
    return result


async def _download(session, url, data, *, remaining):
    # This connection carries only JSON. File contents stay in the receiving pod.
    spec = DownloadSpec.model_validate(data.model_dump()).model_dump()
    spec["timeout_seconds"] = remaining
    async with session.post(url, json=spec, allow_redirects=False) as response:
        payload = await _read_json(response)
    try:
        receipt = DownloadReceipt.model_validate(payload)
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Verifier returned an invalid receipt") from exc
    if (
        receipt.cid != data.cid
        or receipt.sha256 != data.expected_sha256
        or receipt.file_size_bytes != data.expected_size_bytes
        or receipt.transport != data.transport
    ):
        raise HTTPException(status_code=409, detail="Verifier receipt does not match the requested file")
    return receipt.model_dump()


async def _run_operation(operation, request, data, config):
    try:
        deadline = asyncio.get_running_loop().time() + data.timeout_seconds
        # Includes capacity wait, pod lookup, verifier queue, and the complete stream.
        async with asyncio.timeout(data.timeout_seconds):
            async with request.app.state.storage_semaphore:
                kwargs = (
                    {"port": data.verifier_port, "path": "/download"}
                    if operation == "download"
                    else {}
                )
                pod_name, url = await _resolve_target(data, request, config, **kwargs)
                if operation == "download":
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise TimeoutError
                    receipt = await _download(
                        request.app.state.storage_session, url, data, remaining=remaining
                    )
                else:
                    receipt = await _upload(request.app.state.storage_session, url, data)
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
