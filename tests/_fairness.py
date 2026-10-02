from __future__ import annotations

import httpcore
from httpcore._async.connection_pool import AsyncConnectionPool, AsyncPoolRequest
from httpcore._sync.connection_pool import ConnectionPool, PoolRequest


class FakeConnection:
    def __init__(
        self,
        origin: httpcore.Origin,
        *,
        available: bool = False,
        idle: bool = False,
        failing: bool = False,
    ) -> None:
        self._origin = origin
        self._available = available
        self._idle = idle
        self._failing = failing
        self._closed = False

    def can_handle_request(self, origin: httpcore.Origin) -> bool:
        return origin == self._origin

    def is_available(self) -> bool:
        return self._available and not self._closed

    def is_idle(self) -> bool:
        return self._idle and not self._closed

    def is_multiplexed(self) -> bool:
        return self._available and not self._idle

    def has_expired(self) -> bool:
        return False

    def is_closed(self) -> bool:
        return self._closed

    def handle_request(self, request: httpcore.Request) -> httpcore.Response:
        self._closed = self._failing
        if self._failing:
            raise httpcore.ConnectError("Could not connect")
        raise AssertionError("Unexpected fake connection request")

    async def handle_async_request(
        self, request: httpcore.Request
    ) -> httpcore.Response:
        return self.handle_request(request)

    def close(self) -> None:
        self._closed = True

    async def aclose(self) -> None:
        self.close()


def add_request(
    pool: object, host: str, connection: FakeConnection | None = None
) -> object:
    request_class = AsyncPoolRequest if isinstance(pool, AsyncConnectionPool) else PoolRequest
    pool_request = request_class(httpcore.Request("GET", f"http://{host}/"))
    if connection is not None:
        pool_request.connection = connection
    pool._requests.append(pool_request)
    return pool_request


def install_fake_connections(pool: object, created: list[str]) -> None:
    def create_connection(origin: httpcore.Origin) -> FakeConnection:
        host = origin.host.decode()
        created.append(host)
        return FakeConnection(origin, failing=host == "a.com")

    pool.create_connection = create_connection


def origin(host: str) -> httpcore.Origin:
    return httpcore.URL(f"http://{host}/").origin
