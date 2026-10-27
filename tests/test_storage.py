import asyncio
import hashlib
import json
import time
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
from routers import storage, storage_verifier


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.stack = AsyncExitStack()
        self.addAsyncCleanup(self.stack.aclose)
        self.payload = bytes(range(256)) * 1031
        self.upload_status = 200
        self.upload_reply = b"test-cid\n"
        self.stream_status = 200
        self.declared_length = None
        self.omit_length = False
        self.truncate = False
        self.verifier_reply = None
        self.verifier_payloads = []
        self.forbidden_requests = []
        self.stream_requests = 0
        self.active_streams = 0
        self.peak_streams = 0
        self.stream_gate = None
        self.stream_started = asyncio.Event()
        upstream = web.Application()
        upstream.router.add_post("/api/storage/v1/data", self.upload)
        upstream.router.add_get("/api/storage/v1/data/{cid}/network/stream", self.stream)
        upstream.router.add_route("*", "/{path:.*}", self.forbidden)
        self.server = await self.stack.enter_async_context(TestServer(upstream))

        # The real verifier runs through ASGI; this HTTP bridge stands in for its pod port.
        self.verifier_app = storage_verifier.create_app(storage_port=self.server.port)
        await self.stack.enter_async_context(
            self.verifier_app.router.lifespan_context(self.verifier_app)
        )
        self.verifier_client = await self.stack.enter_async_context(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.verifier_app), base_url="http://verifier"
        ))
        verifier_http = web.Application()
        verifier_http.router.add_post("/download", self.verify)
        self.verifier_server = await self.stack.enter_async_context(TestServer(verifier_http))

        self.target = ConfigTarget(
            name="storage-node-0", name_template="^storage-node-0$", port=self.server.port
        )
        self.pod = V1Pod(
            metadata=V1ObjectMeta(name="storage-node-0"),
            status=V1PodStatus(pod_ip="127.0.0.1"),
        )
        self.pods = V1PodList(items=[self.pod])
        self.lookup = Mock(return_value=self.pods)
        self.stack.enter_context(patch(
            "common.get_core_v1", return_value=Mock(list_namespaced_pod=self.lookup)
        ))

        async def get_config():
            return {"targets": {self.target.name: self.target}}

        self.app = FastAPI()
        self.app.state.namespace = "test-storage"
        self.app.state.cache = defaultdict(dict)
        self.app.include_router(storage.create_router(get_config, max_concurrent_operations=2))
        await self.stack.enter_async_context(self.app.router.lifespan_context(self.app))
        self.client = await self.stack.enter_async_context(httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://requester"
        ))

    async def upload(self, request):
        self.upload_headers = request.headers
        body = bytearray()
        async for chunk in request.content.iter_chunked(16381):
            body.extend(chunk)
        self.payload = bytes(body)
        return web.Response(status=self.upload_status, body=self.upload_reply)

    async def forbidden(self, request):
        self.forbidden_requests.append((request.method, request.path))
        return web.Response(status=500, text="Background and local-read endpoints must not be used")

    async def stream(self, request):
        self.stream_requests += 1
        self.stream_path = request.path
        self.download_query = dict(request.query)
        self.accept_encoding = request.headers.get("Accept-Encoding")
        if self.stream_status != 200:
            return web.Response(status=self.stream_status, body=b"x" * 10000)
        self.active_streams += 1
        self.peak_streams = max(self.peak_streams, self.active_streams)
        self.stream_started.set()
        try:
            if self.stream_gate is not None:
                await self.stream_gate.wait()
            headers = {} if self.omit_length else {
                "Content-Length": str(self.declared_length or len(self.payload))
            }
            response = web.StreamResponse(headers=headers)
            await response.prepare(request)
            if self.truncate:
                await response.write(self.payload[:100])
                request.transport.close()
                return response
            for offset in range(0, len(self.payload), 16381):
                await response.write(self.payload[offset:offset + 16381])
            await response.write_eof()
            return response
        except ConnectionResetError:
            return web.Response(status=499)
        finally:
            self.active_streams -= 1

    async def verify(self, request):
        data = await request.json()
        self.verifier_payloads.append(data)
        if self.verifier_reply is not None:
            return web.Response(body=self.verifier_reply, content_type="application/json")
        response = await self.verifier_client.post("/download", json=data)
        return web.Response(
            status=response.status_code, body=response.content, content_type="application/json"
        )

    def upload_data(self, **overrides):
        return {
            "target": {"kind": "name", "value": self.target.name},
            "file_size_bytes": 2 * storage.CHUNK_SIZE + 17,
            **overrides,
        }

    def download_data(self, **overrides):
        return {
            "target": {"kind": "name", "value": self.target.name},
            "verifier_port": self.verifier_server.port,
            "cid": "test-cid",
            "expected_size_bytes": len(self.payload),
            "expected_sha256": hashlib.sha256(self.payload).hexdigest(),
            **overrides,
        }

    async def test_upload_then_verified_stream_in_the_receiver_pod(self):
        response = await self.client.post("/storage/upload", json=self.upload_data())
        self.assertEqual(response.status_code, 200, response.text)
        uploaded = json.loads(response.json()["response"]["text"])
        self.assertEqual(uploaded["sha256"], hashlib.sha256(self.payload).hexdigest())
        self.assertEqual(uploaded["file_size_bytes"], 2 * storage.CHUNK_SIZE + 17)
        self.assertEqual(int(self.upload_headers["Content-Length"]), len(self.payload))
        self.assertNotIn("Transfer-Encoding", self.upload_headers)
        # A whole-response read would defeat bounded-memory streaming and fails this check.
        with patch("aiohttp.ClientResponse.read", side_effect=AssertionError("Whole-body read")):
            response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 200, response.text)
        result = response.json()
        self.assertEqual(result["request"]["pod"], "storage-node-0")
        self.assertEqual(result["request"]["url"], f"http://127.0.0.1:{self.verifier_server.port}/download")
        receipt = json.loads(result["response"]["text"])
        self.assertEqual(receipt["sha256"], uploaded["sha256"])
        self.assertEqual(receipt["file_size_bytes"], uploaded["file_size_bytes"])
        self.assertTrue(receipt["verified"])
        self.assertEqual(receipt["verification_location"], "receiver_pod")
        self.assertEqual(receipt["transport"], "direct")
        self.assertEqual(self.stream_path, "/api/storage/v1/data/test-cid/network/stream")
        self.assertEqual(self.stream_requests, 1)
        self.assertEqual(self.download_query, {"transport": "direct"})
        self.assertEqual(self.accept_encoding, "identity")
        self.assertEqual(self.forbidden_requests, [])
        self.assertNotIn("target", self.verifier_payloads[0])
        self.assertLess(len(result["response"]["text"]), 1024)
        self.lookup.assert_called_once_with("test-storage")

    async def test_inline_target_and_mix_transport(self):
        response = await self.client.post("/storage/download", json=self.download_data(
            target={"kind": "config", "value": self.target.model_dump()}, transport="mix"
        ))
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.download_query, {"transport": "mix"})
        self.assertEqual(json.loads(response.json()["response"]["text"])["transport"], "mix")

    async def test_hash_and_size_mismatches_never_return_verified_receipts(self):
        for overrides in (
            {"expected_sha256": "0" * 64}, {"expected_size_bytes": len(self.payload) + 1}
        ):
            with self.subTest(overrides=overrides):
                response = await self.client.post("/storage/download", json=self.download_data(**overrides))
                self.assertEqual(response.status_code, 502, response.text)
                self.assertEqual(response.json()["detail"]["upstream_status_code"], 409)

    async def test_truncated_content_length_is_an_error(self):
        self.truncate = True
        response = await self.client.post("/storage/download", json=self.download_data(timeout_seconds=2))
        self.assertEqual(response.status_code, 502, response.text)
        self.assertNotIn("response", response.json())

    async def test_chunked_stream_still_requires_exact_byte_count_and_hash(self):
        self.omit_length = True
        response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 200, response.text)
        response = await self.client.post("/storage/download", json=self.download_data(
            expected_size_bytes=len(self.payload) - 1
        ))
        self.assertEqual(response.status_code, 502, response.text)
        self.assertEqual(response.json()["detail"]["upstream_status_code"], 409)

    async def test_missing_or_incorrect_verifier_does_not_fall_back_to_central_streaming(self):
        data = self.download_data()
        await self.verifier_server.close()
        response = await self.client.post("/storage/download", json=data)
        self.assertEqual(response.status_code, 502, response.text)
        self.assertEqual(self.stream_requests, 0)
        self.assertEqual(self.forbidden_requests, [])

    async def test_invalid_or_oversized_verifier_json_is_rejected(self):
        for body in (b"invalid", b"[]", b"{}", b"x" * (storage.MAX_JSON_BYTES + 1)):
            with self.subTest(length=len(body)):
                self.verifier_reply = body
                response = await self.client.post("/storage/download", json=self.download_data())
                self.assertEqual(response.status_code, 502, response.text)
        self.assertEqual(self.stream_requests, 0)

    async def test_verifier_receipt_must_match_the_requested_file(self):
        receipt = {
            "cid": "another-cid", "sha256": "0" * 64,
            "file_size_bytes": len(self.payload), "seconds": 0.1, "transport": "direct",
            "verified": True, "verification_location": "receiver_pod",
        }
        self.verifier_reply = json.dumps(receipt).encode()
        response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 409, response.text)

    async def test_invalid_requests_are_rejected_before_contacting_a_verifier(self):
        for overrides in (
            {"expected_size_bytes": 0}, {"expected_sha256": "bad"}, {"expected_sha256": None},
            {"cid": " "}, {"transport": "unknown"}, {"timeout_seconds": 0},
            {"timeout_seconds": "Infinity"}, {"verifier_port": 0}, {"verifier_port": 65536},
        ):
            with self.subTest(overrides=overrides):
                response = await self.client.post("/storage/download", json=self.download_data(**overrides))
                self.assertEqual(response.status_code, 422, response.text)
        self.assertEqual(self.verifier_payloads, [])

    async def test_missing_named_target(self):
        response = await self.client.post("/storage/upload", json=self.upload_data(
            target={"kind": "name", "value": "missing"}
        ))
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
        self.stream_status = 500
        response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 502, response.text)
        self.assertLessEqual(len(response.json()["detail"]["body"]), storage.MAX_ERROR_BYTES)
        self.upload_status = 400
        response = await self.client.post("/storage/upload", json=self.upload_data())
        self.assertEqual(response.status_code, 502)

    async def test_original_storage_status_survives_the_verifier_hop(self):
        self.stream_status = 400
        response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 502, response.text)
        self.assertEqual(response.json()["detail"]["upstream_status_code"], 400)

    async def test_kubernetes_lookup_failure(self):
        self.lookup.side_effect = ApiException(status=403, reason="Forbidden")
        response = await self.client.post("/storage/download", json=self.download_data())
        self.assertEqual(response.status_code, 503)

    async def test_invalid_upload_receipts(self):
        for body in (b"", b"\xff", b"x" * (storage.MAX_CID_BYTES + 1)):
            with self.subTest(length=len(body)):
                self.upload_reply = body
                response = await self.client.post("/storage/upload", json=self.upload_data())
                self.assertEqual(response.status_code, 502, response.text)

    async def test_verifier_receives_remaining_deadline_after_pod_lookup(self):
        def slow_lookup(*_args, **_kwargs):
            time.sleep(0.03)
            return self.pods

        self.lookup.side_effect = slow_lookup
        response = await self.client.post("/storage/download", json=self.download_data(timeout_seconds=2))
        self.assertEqual(response.status_code, 200, response.text)
        self.assertGreater(self.verifier_payloads[0]["timeout_seconds"], 0)
        self.assertLess(self.verifier_payloads[0]["timeout_seconds"], 1.99)

    async def test_per_pod_limit_and_central_queued_request_deadline(self):
        self.stream_gate = asyncio.Event()
        pending = [asyncio.create_task(self.client.post(
            "/storage/download", json=self.download_data()
        )) for _ in range(2)]
        try:
            async with asyncio.timeout(2):
                await self.stream_started.wait()
                while len(self.verifier_payloads) < 2:
                    await asyncio.sleep(0)
            response = await self.client.post("/storage/download", json=self.download_data(timeout_seconds=0.05))
            self.assertEqual(response.status_code, 504, response.text)
            self.assertEqual(self.peak_streams, 1)
        finally:
            self.stream_gate.set()
            responses = await asyncio.gather(*pending)
        self.assertTrue(all(response.status_code == 200 for response in responses))

    async def test_local_verifier_timeout_releases_its_slot(self):
        self.stream_gate = asyncio.Event()
        data = self.download_data(timeout_seconds=0.05)
        data.pop("target")
        data.pop("verifier_port")
        try:
            response = await self.verifier_client.post("/download", json=data)
            self.assertEqual(response.status_code, 504, response.text)
        finally:
            self.stream_gate.set()
        data["timeout_seconds"] = 2
        response = await self.verifier_client.post("/download", json=data)
        self.assertEqual(response.status_code, 200, response.text)

    async def test_verifier_health_and_session_cleanup_need_no_kubernetes(self):
        response = await self.verifier_client.get("/healthz")
        self.assertEqual(response.status_code, 200)
        self.lookup.assert_not_called()
        central = self.app.state.storage_session
        local = self.verifier_app.state.storage_session
        await self.stack.aclose()
        self.assertTrue(central.closed)
        self.assertTrue(local.closed)


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
