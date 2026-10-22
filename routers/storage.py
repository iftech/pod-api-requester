"""Logos Storage uploads and background downloads, controlled by the experiment."""

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
MAX_JSON_BYTES = 64 * 1024
MAX_CONCURRENT_OPERATIONS = 8


class StorageRequest(BaseModel):
    target: Annotated[Union[TargetName, TargetConfig], Field(discriminator="kind")]
    timeout_seconds: float = Field(default=60, gt=0, allow_inf_nan=False)


class UploadRequest(StorageRequest):
    file_size_bytes: PositiveInt = 1024 * 1024


class DownloadRequest(StorageRequest):
    cid: str = Field(min_length=1, max_length=MAX_CID_BYTES)
    expected_size_bytes: PositiveInt
    transport: Literal["direct", "mix"] = "direct"
    poll_interval_seconds: float = Field(default=1, gt=0, allow_inf_nan=False)

    @field_validator("cid")
    @classmethod
    def strip_cid(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("CID cannot be blank")
        return value


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


async def _read_json(response):
    await _check_status(response)
    body = bytearray()
    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
        body.extend(chunk)
        if len(body) > MAX_JSON_BYTES:
            raise HTTPException(status_code=502, detail="Storage node returned oversized JSON")
    try:
        result = json.loads(body)
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Storage node returned invalid JSON") from exc
    if not isinstance(result, dict):
        raise HTTPException(status_code=502, detail="Storage node returned a non-object JSON value")
    return result


async def _cancel_download(session, url):
    # Cancellation is best effort and has its own small budget after the operation expires.
    try:
        async with asyncio.timeout(2):
            async with session.delete(url, allow_redirects=False) as response:
                await response.content.read(MAX_ERROR_BYTES)
    except (Exception, asyncio.CancelledError):
        pass


async def _download(session, url, data):
    started = time.perf_counter()
    async with session.post(
        url, params={"transport": data.transport}, allow_redirects=False
    ) as response:
        download = await _read_json(response)

    manifest = download.get("manifest")
    download_id = download.get("downloadId")
    tree_cid = manifest.get("treeCid") if isinstance(manifest, dict) else None
    if (
        type(download_id) is not int
        or download_id < 1
        or not isinstance(tree_cid, str)
        or not tree_cid.strip()
        or len(tree_cid) > MAX_CID_BYTES
    ):
        raise HTTPException(
            status_code=502, detail="Storage node returned an invalid download ID or tree CID"
        )

    # The current Storage engine indexes active downloads by the manifest's tree CID.
    # Polling with the manifest CID would incorrectly report inactive immediately.
    data_url = url.rsplit("/", 2)[0]
    job_url = f"{data_url}/{quote(tree_cid, safe='')}/network"
    progress_url = f"{job_url}/progress/{download_id}"
    cancel_url = f"{job_url}/{download_id}"
    polls = 0
    last_active_progress = None
    try:
        if download.get("cid") != data.cid:
            raise HTTPException(status_code=502, detail="Storage node returned a different CID")
        dataset_size = manifest.get("datasetSize")
        if type(dataset_size) is not int or dataset_size != data.expected_size_bytes:
            raise HTTPException(
                status_code=409, detail="Manifest size does not match the expected size"
            )
        while True:
            async with session.get(progress_url, allow_redirects=False) as response:
                progress = await _read_json(response)
            polls += 1
            active = progress.get("active")
            if active is False:
                # The API removes finished jobs and has no terminal success/error status.
                # Inactivity is observable here; payload integrity is deliberately unverified.
                return {
                    "cid": data.cid,
                    "tree_cid": tree_cid,
                    "download_id": download_id,
                    "dataset_size_bytes": dataset_size,
                    "seconds": time.perf_counter() - started,
                    "progress_polls": polls,
                    "last_active_progress": last_active_progress,
                    "completion": "inactive",
                    "verified": False,
                }
            if active is not True:
                raise HTTPException(
                    status_code=502, detail="Storage progress is missing a boolean active flag"
                )
            if any(
                type(progress.get(key)) is not int or progress[key] < 0
                for key in ("received", "total", "bytes")
            ):
                raise HTTPException(
                    status_code=502, detail="Storage node returned invalid progress counters"
                )
            if progress["received"] > progress["total"]:
                raise HTTPException(
                    status_code=502, detail="Storage progress exceeds its total block count"
                )
            last_active_progress = progress
            await asyncio.sleep(data.poll_interval_seconds)
    except (Exception, asyncio.CancelledError):
        await _cancel_download(session, cancel_url)
        raise


async def _run_operation(operation, request, data, config):
    try:
        # Includes capacity wait, pod lookup, and the complete HTTP exchange.
        async with asyncio.timeout(data.timeout_seconds):
            async with request.app.state.storage_semaphore:
                pod_name, url = await _resolve_target(data, request, config)
                if operation == "download":
                    url += f"/{quote(data.cid, safe='')}/network"
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
