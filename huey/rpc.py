import aiohttp


class BaseComm(object):
    def __init__(self, name='huey'):
        self.name = name

    def close(self):
        """
        Close or release any objects/handles used by storage layer.

        :returns: (optional) boolean indicating success
        """
        pass

    def send(self):
        pass

    def recv(self):
        pass

    async def send_async(self):
        pass

    async def recv_async(self):
        pass


class HTTPComm(BaseComm):
    def __init__(self, name, host, port):
        super(HTTPComm, self).__init__(name)
        self.conn = aiohttp.TCPConnector(verify_ssl=False)
        self.host_port = f"{host}:{port}"

    async def comm_async(self, endpoint: str, data: bytes) -> bytes:
        async with aiohttp.ClientSession(connector=self.conn) as session:
            async with session.get(f"{self.host_port}/{endpoint}") as r:
                if r._body is None:  # noqa
                    await r.read()
                return r._body  # noqa
