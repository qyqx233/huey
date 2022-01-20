import asyncio
import inspect
from typing import List, Optional

from loguru import logger
from pydantic import BaseModel

from huey.rpc import HTTPComm, PydanticRpc, JSONRpc


class CmdRequest(BaseModel):
    name: str
    args: Optional[List[str]]


class CmdResponse(BaseModel):
    code: int
    msg: str
    data: Optional[str]


def test_pydantic():
    cmd = CmdRequest(name="aa", args=["ls"])
    print(cmd.json().__class__)
    print(cmd.json())


def test():
    async def fx():
        client = PydanticRpc(HTTPComm("192.168.50.76", 3212))
        r = await client.request("/", CmdRequest(name="ls"), CmdResponse)
        logger.debug("xxx {}", r.__class__)

    asyncio.get_event_loop().run_until_complete(fx())


def add(a: int, b: int) -> int:
    return a + b


def w(a):
    def deco(fn):
        def wrapper(*args):
            return fn(*args)

        return wrapper

    return deco


def ww(a):
    class Deco:
        def __init__(self, fn):
            self.fn = fn
            print()
            print("ssssss", asyncio.iscoroutinefunction(fn))

        def __call__(self, *args, ):
            return self.fn(*args)

    return Deco


# @ww(100)
async def foo(a, b, *v, c=100):
    return a + b


def test_add():
    async def fx():
        sig = inspect.signature(foo)
        print()
        # print(await foo(1, 2))

        for k in sig.parameters:
            v = sig.parameters[k]
            print("==", k, v.name, v.empty, v.default, v.annotation)

    asyncio.get_event_loop().run_until_complete(fx())


def throw_non_200(j):
    code = j["code"]
    if code != 200:
        raise Exception(j["msg"])
    logger.debug(j)
    return j["data"]


rpc = JSONRpc(HTTPComm("192.168.50.76", 3212), post_hook=throw_non_200)


def no_arg():
    pass


@rpc.call_wrapper("/", )
def ff(name: str, args: List[str]):
    pass


def test_send():
    async def fx():
        logger.debug(await ff("ls", ["/"]))
        # logger.debug(inspect.signature(no_arg).__class__)
        pass

    asyncio.get_event_loop().run_until_complete(fx())
