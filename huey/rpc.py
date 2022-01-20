import abc
import functools
import inspect
import json
from typing import Any, Type

import aiohttp
from loguru import logger
from pydantic import BaseModel


class Comm(object):
    def __init__(self):
        pass

    def close(self):
        """
        Close or release any objects/handles used by storage layer.

        :returns: (optional) boolean indicating success
        """
        pass

    def exchange_sync(self):
        pass

    async def exchange(self, endpoint: str, data: bytes) -> bytes:
        pass

    async def recv(self):
        pass


class HTTPComm(Comm):
    def __init__(self, host, port):
        super(HTTPComm, self).__init__()
        self.conn = aiohttp.TCPConnector(verify_ssl=False)
        self.host_port = f"http://{host}:{port}"  # noqa

    async def exchange(self, endpoint: str, data: bytes) -> bytes:
        async with aiohttp.ClientSession(connector=self.conn) as session:
            target = f"{self.host_port}{endpoint}"
            async with session.post(target, data=data) as r:
                if r._body is None:  # noqa
                    await r.read()
                return r._body  # noqa


class Serializer:
    @abc.abstractmethod
    def serialize(self, obj: Any) -> bytes:
        pass

    @abc.abstractmethod
    def deserialize(self, data: bytes, t: Type = None) -> Any:
        pass


class PydanticSerializer(Serializer):
    def serialize(self, obj: BaseModel) -> bytes:
        return obj.json().encode("utf8")

    def deserialize(self, data: bytes, t: BaseModel = None) -> BaseModel:
        return t.parse_obj(json.loads(data.decode("utf8")))


class JSONSerializer(Serializer):
    def __init__(self):
        Serializer.__init__(self)

    def serialize(self, obj: Any) -> bytes:
        return json.dumps(obj).encode("utf8")

    def deserialize(self, data: bytes, t: Type = None) -> Any:
        return json.loads(data.decode("utf8"))


class CallWrapper:
    def __call__(self, func):
        pass


class Rpc:
    serializer: Serializer
    comm: Comm

    def __init__(self):
        pass

    # def serialize(self, obj):
    #     return self.serializer.serialize(obj)

    async def request(self, endpoint, obj, t: Any):
        serializer = self.serializer
        req_data = serializer.serialize(obj)
        logger.debug("req_data={}", req_data)
        rsp_data = await self.comm.exchange(endpoint, req_data)
        logger.debug(rsp_data)
        return serializer.deserialize(rsp_data, t)

    def call_wrapper(self, endpoint, response_class: Any = None):
        that = self

        class Deco:
            def __init__(self, fn):
                self.sig = inspect.signature(fn)
                print(self.sig)
                self.fn = fn
                self.rpc = that

            async def __call__(self, *args):
                req = {k: v for k, v in zip(self.sig.parameters, args)}
                return await self.rpc.request(endpoint, req, response_class)

        return Deco


class JSONRpc(Rpc):
    def __init__(self, comm):
        Rpc.__init__(self)
        self.serializer = JSONSerializer()
        self.comm = comm


class PydanticRpc(Rpc):
    def __init__(self, comm):
        Rpc.__init__(self)
        self.serializer = PydanticSerializer()
        self.comm = comm