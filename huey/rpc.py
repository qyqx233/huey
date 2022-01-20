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
        async with aiohttp.ClientSession() as session:
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

    def __init__(self, pre_hook=None, post_hook=None, error_log=True, debug_request=False):
        self.post_hook = post_hook
        self.pre_hook = pre_hook
        self.error_log = error_log
        self.debug_request = debug_request

    # def serialize(self, obj):
    #     return self.serializer.serialize(obj)

    async def request(self, endpoint, request, t: Any):
        serializer = self.serializer
        rsp_data = b''
        if self.debug_request:
            logger.debug(request)
        try:
            if self.pre_hook:
                self.pre_hook(request)
            req_data = serializer.serialize(request)
            rsp_data = await self.comm.exchange(endpoint, req_data)
            response = serializer.deserialize(rsp_data, t)
            r = self.post_hook(response)
            return r
        except Exception as e:
            logger.error(f"error: {e}, rsp_data={rsp_data}")
            raise e

    def call_wrapper(self, endpoint, response_class: Any = None):
        that = self

        class Deco:
            def __init__(self, fn):
                self.sig = inspect.signature(fn)
                defaults = {}
                for k, v in self.sig.parameters.items():
                    if v.default is not inspect.Signature.empty:
                        defaults[k] = v.default
                self.names = list(self.sig.parameters)
                self.defaults = defaults
                self.fn = fn
                self.rpc = that

            async def __call__(self, *args):
                if self.defaults:
                    req = self.defaults.copy()
                    req.update({k: v for k, v in zip(self.names[:len(args)], args)})
                else:
                    req = {k: v for k, v in zip(self.sig.parameters, args)}
                return await self.rpc.request(endpoint, req, response_class)

        return Deco


class JSONRpc(Rpc):
    def __init__(self, comm, pre_hook=None, post_hook=None, error_log=True, debug_request=False):
        Rpc.__init__(self, pre_hook, post_hook, error_log, debug_request)
        self.serializer = JSONSerializer()
        self.comm = comm
        self.post_hook = post_hook


class PydanticRpc(Rpc):
    def __init__(self, comm):
        Rpc.__init__(self)
        self.serializer = PydanticSerializer()
        self.comm = comm
