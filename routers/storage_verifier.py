"""Hash Storage's network stream inside the receiving pod; return only a receipt."""

import argparse
import asyncio
import hashlib
import time
from contextlib import asynccontextmanager
from typing import Literal
from urllib.parse import quote

import aiohttp
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, PositiveInt, field_validator

from routers.probe_logging import suppress_successful_probe_logs

CHUNK_SIZE = 64 * 1024
MAX_CID_BYTES = 2048
MAX_ERROR_BYTES = 4096
VERIFIER_PORT = 8646


class DownloadSpec(BaseModel):
    cid: str = Field(min_length=1, max_length=MAX_CID_BYTES)
    expected_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_size_bytes: PositiveInt
    transport: Literal["direct", "mix"] = "direct"
    timeout_seconds: float = Field(default=600, gt=0, allow_inf_nan=False)

    @field_validator("cid")
    @classmethod
    def strip_cid(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("CID cannot be blank")
        return value


class DownloadReceipt(BaseModel):
    cid: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_size_bytes: PositiveInt
    seconds: float = Field(ge=0, allow_inf_nan=False)
    transport: Literal["direct", "mix"]
    verified: Literal[True]
    verification_location: Literal["receiver_pod"]


async def check_status(response):
    if response.status != 200:
        body = await response.content.read(MAX_ERROR_BYTES)
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Storage node or verifier returned an error",
                "upstream_status_code": response.status,
                "body": body.decode("utf-8", errors="replace"),
            },
        )


async def download_and_verify(session, url: str, data: DownloadSpec) -> dict:
    """One streaming pass, with neither a payload file nor a full-body buffer."""
    digest = hashlib.sha256()
    received = 0
    started = time.perf_counter()
    async with session.get(
        url, params={"transport": data.transport}, allow_redirects=False
    ) as response:
        await check_status(response)
        if (
            response.content_length is not None
            and response.content_length != data.expected_size_bytes
        ):
            raise HTTPException(
                status_code=409, detail="Storage Content-Length does not match expected size"
            )
        # aiohttp also raises ClientPayloadError if a declared body ends early.
        async for chunk in response.content.iter_chunked(CHUNK_SIZE):
            received += len(chunk)
            if received > data.expected_size_bytes:
                raise HTTPException(status_code=409, detail="Downloaded file exceeds expected size")
            digest.update(chunk)

    if received != data.expected_size_bytes:
        raise HTTPException(
            status_code=409, detail="Downloaded file size does not match expected size"
        )
    actual_sha256 = digest.hexdigest()
    if actual_sha256 != data.expected_sha256:
        raise HTTPException(status_code=409, detail="Downloaded file SHA-256 does not match upload")
    return DownloadReceipt(
        cid=data.cid,
        sha256=actual_sha256,
        file_size_bytes=received,
        seconds=time.perf_counter() - started,
        transport=data.transport,
        verified=True,
        verification_location="receiver_pod",
    ).model_dump()


def create_app(*, storage_port: int = 8080, max_concurrent_downloads: int = 1) -> FastAPI:
    if not 1 <= storage_port <= 65535 or max_concurrent_downloads < 1:
        raise ValueError("Storage port and download concurrency must be valid positive values")

    @asynccontextmanager
    async def lifespan(app):
        suppress_successful_probe_logs()
        app.state.download_slots = asyncio.Semaphore(max_concurrent_downloads)
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=max_concurrent_downloads),
            timeout=aiohttp.ClientTimeout(total=None, connect=10),
            headers={"Accept-Encoding": "identity"},
            auto_decompress=False,
        ) as session:
            app.state.storage_session = session
            yield

    app = FastAPI(lifespan=lifespan)

    @app.get("/healthz")
    async def health():
        return {"status": "ok"}

    @app.post("/download")
    async def download(data: DownloadSpec, request: Request):
        # The destination is fixed to this pod. Callers cannot supply a remote URL.
        url = (
            f"http://127.0.0.1:{storage_port}/api/storage/v1/data/"
            f"{quote(data.cid, safe='')}/network/stream"
        )
        try:
            async with asyncio.timeout(data.timeout_seconds):
                async with request.app.state.download_slots:
                    return await download_and_verify(request.app.state.storage_session, url, data)
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504, detail="Storage stream verification timed out"
            ) from exc
        except aiohttp.ClientError as exc:
            raise HTTPException(status_code=502, detail=f"Storage stream failed: {exc}") from exc

    return app


def main():
    import uvicorn

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=VERIFIER_PORT)
    parser.add_argument("--storage-port", type=int, default=8080)
    parser.add_argument("--max-concurrent-downloads", type=int, default=1)
    args = parser.parse_args()
    uvicorn.run(
        create_app(
            storage_port=args.storage_port,
            max_concurrent_downloads=args.max_concurrent_downloads,
        ),
        host="0.0.0.0",
        port=args.port,
    )


if __name__ == "__main__":
    main()
