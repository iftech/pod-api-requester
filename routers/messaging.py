import base64
import os
from typing import Annotated, Awaitable, Callable, Union

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field, NonNegativeInt

from common import call_endpoint, get_pod_infos
from configs import ConfigEndpoint
from routers.deps import TargetConfig, TargetName, endpoint_error_handler, unwrap_arg
from schemas import NotFoundError
from utils import redact_keys, setup_logger

logger = setup_logger(__file__)


def get_endpoint_messaging_send(
    content_topic: str,
    port: NonNegativeInt,
    msg_size_kbytes: NonNegativeInt,
) -> ConfigEndpoint:
    payload = base64.b64encode(os.urandom(msg_size_kbytes * 1000)).decode("ascii")
    return ConfigEndpoint(
        name="dummy",
        paged=False,
        type="POST",
        url=f"http://{{node}}:{port}/messaging/v1/messages",
        headers={"Content-Type": "application/json"},
        params={"payload": payload, "contentTopic": content_topic},
    )


class MessagingSendData(BaseModel):
    target: Annotated[Union[TargetName, TargetConfig], Field(discriminator="kind")]
    content_topic: str
    port: NonNegativeInt
    msg_size_kbytes: NonNegativeInt


def create_router(get_config: Callable[[], Awaitable[dict]]) -> APIRouter:
    router = APIRouter()

    @router.post("/messaging/send")
    @endpoint_error_handler
    async def messaging_send(
        request: Request,
        data: MessagingSendData,
        config=Depends(get_config),
    ):
        target = unwrap_arg(data.target, "targets", config)

        try:
            pods = get_pod_infos(
                targets=[target],
                namespace=request.app.state.namespace,
                cache=request.app.state.cache,
            )
            pod_info = next(iter(pods))
        except StopIteration as e:
            raise NotFoundError(f"Target not found. Target: {target}") from e

        endpoint = get_endpoint_messaging_send(
            content_topic=data.content_topic,
            port=data.port,
            msg_size_kbytes=data.msg_size_kbytes,
        )
        result = call_endpoint(endpoint, pod_info)
        result = redact_keys(result, keys_to_redact=("payload",))
        configEndpoint = result["request"]["configEndpoint"]
        configEndpoint.params = redact_keys(configEndpoint.params, keys_to_redact=("payload",))
        return result

    return router
