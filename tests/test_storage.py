import asyncio
import hashlib
import json
import unittest
from collections import defaultdict
from contextlib import AsyncExitStack
from unittest.mock import Mock, mock_open, patch

import httpx
from aiohttp import web
from aiohttp.test_utils import TestServer
from fastapi import FastAPI
from kubernetes.client import V1ObjectMeta, V1Pod, V1PodList, V1PodStatus
from kubernetes.client.exceptions import ApiException

from configs import ConfigTarget
from routers import storage


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = AsyncExitStack()
        self.addAsyncCleanup(self.stack.aclose)
        self.payload = bytes(range(256)) * 1031
        self.upload_status = 200
        self.upload_reply = b"test-cid\n"
        self.download_status = 200
        self.download_delay = 0
        self.active_downloads = 0
        self.peak_downloads = 0
        self.download_gate = None
        self.download_started = asyncio.Event()
        upstream = web.Application()
        upstream.router.add_post("/api/storage/v1/data", self.upload)
        upstream.router.add_get("/api/storage/v1/data/{cid}/network/stream", self.download)
        self.server = await self.stack.enter_async_context(TestServer(upstream))
        self.target = ConfigTarget(
            name="storage-node-0", name_template="^storage-node-0$", port=self.server.port
        )
        self.pod = V1Pod(
            metadata=V1ObjectMeta(name="storage-node-0"),
            status=V1PodStatus(pod_ip="127.0.0.1"),
        )
        self.pods = V1PodList(items=[self.pod])
        self.lookup = Mock(return_value=self.pods)
        self.stack.enter_context(
            patch("common.get_core_v1", return_value=Mock(list_namespaced_pod=self.lookup))
        )

        async def get_config():
            return {"targets": {self.target.name: self.target}}

        self.app = FastAPI()
        self.app.state.namespace = "test-storage"
        self.app.state.cache = defaultdict(dict)
        self.app.include_router(storage.create_router(get_config, max_concurrent_operations=2))
        await self.stack.enter_async_context(self.app.router.lifespan_context(self.app))
        self.client = await self.stack.enter_async_context(
            httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://requester")
        )

    async def upload(self, request):
        self.upload_headers = request.headers
        body = bytearray()
        async for chunk in request.content.iter_chunked(16381):
            body.extend(chunk)
        self.payload = bytes(body)
        return web.Response(status=self.upload_status, body=self.upload_reply)

    async def download(self, request):
        self.download_query = dict(request.query)
        self.download_cid = request.match_info["cid"]
        self.active_downloads += 1
        self.peak_downloads = max(self.peak_downloads, self.active_downloads)
        self.download_started.set()
        try:
            if self.download_gate is not None:
                await self.download_gate.wait()
            await asyncio.sleep(self.download_delay)
            if self.download_status != 200:
                return web.Response(status=self.download_status, body=b"x" * 10000)
            response = web.StreamResponse()
            await response.prepare(request)
            try:
                for offset in range(0, len(self.payload), 7001):
                    await response.write(self.payload[offset : offset + 7001])
                    await asyncio.sleep(0)
                await response.write_eof()
            except (ConnectionResetError, RuntimeError):
                # A timeout or oversized response deliberately closes the connection.
                pass
            return response
        finally:
            self.active_downloads -= 1

    def upload_data(self, **overrides):
        return {
            "target": {"kind": "name", "value": self.target.name},
            "file_size_bytes": 2 * storage.CHUNK_SIZE + 17,
            **overrides,
        }

    def download_data(self, **overrides):
        return {
            "target": {"kind": "name", "value": self.target.name},
            "cid": "test-cid",
            "expected_sha256": hashlib.sha256(self.payload).hexdigest(),
            "expected_size_bytes": len(self.payload),
            **overrides,
        }

    async def test_upload_and_verified_download_round_trip(self):
        response = await self.client.post("/storage/upload", json=self.upload_data())
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        receipt = json.loads(result["response"]["text"])
        self.assertEqual(result["request"]["pod"], "storage-node-0")
        self.assertEqual(result["response"]["status_code"], 200)
        self.assertEqual(receipt["cid"], "test-cid")
        self.assertEqual(receipt["sha256"], hashlib.sha256(self.payload).hexdigest())
        self.assertEqual(receipt["file_size_bytes"], 2 * storage.CHUNK_SIZE + 17)
        self.assertGreaterEqual(receipt["seconds"], 0)
        self.assertEqual(int(self.upload_headers["Content-Length"]), len(self.payload))
        self.assertNotIn("Transfer-Encoding", self.upload_headers)
        self.assertEqual(self.upload_headers["Content-Type"], "application/octet-stream")
        self.assertEqual(self.upload_headers["Accept-Encoding"], "identity")
        response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 200, response.text)
        downloaded = json.loads(response.json()["response"]["text"])
        self.assertTrue(downloaded["verified"])
        self.assertEqual(downloaded["sha256"], receipt["sha256"])
        self.assertEqual(downloaded["file_size_bytes"], len(self.payload))
        self.assertEqual(self.download_query, {"transport": "direct"})
        self.lookup.assert_called_once_with("test-storage")

    async def test_inline_target_and_mix_transport(self):
        response = await self.client.post(
            "/storage/download",
            json=self.download_data(
                target={"kind": "config", "value": self.target.model_dump()},
                transport="mix",
                expected_sha256=hashlib.sha256(self.payload).hexdigest().upper(),
            ),
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.download_query, {"transport": "mix"})

    async def test_corrupt_short_and_oversized_downloads_fail(self):
        for overrides in (
            {"expected_sha256": "0" * 64},
            {"expected_size_bytes": len(self.payload) + 1},
            {"expected_size_bytes": len(self.payload) - 1},
        ):
            with self.subTest(overrides=overrides):
                response = await self.client.post(
                    "/storage/download", json=self.download_data(**overrides)
                )
                self.assertEqual(response.status_code, 409, response.text)

    async def test_invalid_requests_are_rejected(self):
        for overrides in (
            {"expected_sha256": "invalid"},
            {"expected_size_bytes": 0},
            {"cid": " "},
            {"transport": "unknown"},
            {"timeout_seconds": 0},
            {"timeout_seconds": "Infinity"},
        ):
            with self.subTest(overrides=overrides):
                response = await self.client.post(
                    "/storage/download", json=self.download_data(**overrides)
                )
                self.assertEqual(response.status_code, 422, response.text)
        response = await self.client.post(
            "/storage/upload", json=self.upload_data(file_size_bytes=0)
        )
        self.assertEqual(response.status_code, 422)

    async def test_missing_named_target(self):
        response = await self.client.post(
            "/storage/upload",
            json=self.upload_data(target={"kind": "name", "value": "missing"}),
        )
        self.assertEqual(response.status_code, 404)

    async def test_target_must_match_exactly_one_pod(self):
        for pods, status in (([], 404), ([self.pod, self.pod], 409)):
            with self.subTest(status=status):
                self.app.state.cache.clear()
                self.pods.items = pods
                response = await self.client.post("/storage/upload", json=self.upload_data())
                self.assertEqual(response.status_code, status, response.text)

    async def test_missing_ip_and_invalid_port(self):
        self.pod.status.pod_ip = None
        response = await self.client.post("/storage/upload", json=self.upload_data())
        self.assertEqual(response.status_code, 503)
        self.target.port = 0
        response = await self.client.post("/storage/upload", json=self.upload_data())
        self.assertEqual(response.status_code, 422)

    async def test_upstream_errors_have_bounded_diagnostics(self):
        self.download_status = 500
        response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 502)
        detail = response.json()["detail"]
        self.assertEqual(detail["upstream_status_code"], 500)
        self.assertLessEqual(len(detail["body"]), storage.MAX_ERROR_BYTES)
        self.upload_status = 400
        response = await self.client.post("/storage/upload", json=self.upload_data())
        self.assertEqual(response.status_code, 502)

    async def test_connection_failure(self):
        await self.server.close()
        response = await self.client.post("/storage/upload", json=self.upload_data())
        self.assertEqual(response.status_code, 502)

    async def test_kubernetes_lookup_failure(self):
        self.lookup.side_effect = ApiException(status=403, reason="Forbidden")
        response = await self.client.post("/storage/upload", json=self.upload_data())
        self.assertEqual(response.status_code, 503)

    async def test_invalid_upload_receipts(self):
        for body in (b"", b"\xff", b"x" * (storage.MAX_CID_BYTES + 1)):
            with self.subTest(length=len(body)):
                self.upload_reply = body
                response = await self.client.post("/storage/upload", json=self.upload_data())
                self.assertEqual(response.status_code, 502, response.text)

    async def test_transfer_timeout_and_next_request_succeeds(self):
        self.download_delay = 0.2
        response = await self.client.post(
            "/storage/download", json=self.download_data(timeout_seconds=0.05)
        )
        self.assertEqual(response.status_code, 504, response.text)
        self.download_delay = 0
        response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 200, response.text)

    async def test_concurrency_limit_and_queued_request_deadline(self):
        self.download_gate = asyncio.Event()
        pending = [
            asyncio.create_task(self.client.post("/storage/download", json=self.download_data()))
            for _ in range(2)
        ]
        try:
            async with asyncio.timeout(2):
                while self.active_downloads < 2:
                    self.download_started.clear()
                    await self.download_started.wait()
            response = await self.client.post(
                "/storage/download", json=self.download_data(timeout_seconds=0.05)
            )
            self.assertEqual(response.status_code, 504, response.text)
            self.assertEqual(self.peak_downloads, 2)
        finally:
            self.download_gate.set()
            responses = await asyncio.gather(*pending)
        self.assertTrue(all(response.status_code == 200 for response in responses))

    async def test_session_closes_with_app_lifespan(self):
        session = self.app.state.storage_session
        self.assertFalse(session.closed)
        await self.stack.aclose()
        self.assertTrue(session.closed)


class RegistrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_storage_routes_and_lifespan_are_registered_in_real_app(self):
        from app import create_app
        with patch("builtins.open", mock_open(read_data="test-storage\n")):
            app = create_app({})
        self.assertIn("/storage/upload", app.openapi()["paths"])
        self.assertIn("/storage/download", app.openapi()["paths"])
        async with app.router.lifespan_context(app):
            session = app.state.storage_session
            self.assertFalse(session.closed)
        self.assertTrue(session.closed)
