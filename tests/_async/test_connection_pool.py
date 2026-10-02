import logging
import typing

import anyio
import hpack
import hyperframe.frame
import pytest
import trio as concurrency

import httpcore


class AsyncControlledStream:
    def __init__(
        self,
        order: list[str],
        label: str,
        release: anyio.Event,
        on_close: typing.Callable[[], None],
    ) -> None:
        self._order = order
        self._label = label
        self._release = release
        self._on_close = on_close

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        yield b""

    async def aclose(self) -> None:
        await self._release.wait()
        self._on_close()
        self._order.append(f"release:{self._label}")


class AsyncControlledConnection(httpcore.AsyncConnectionInterface):
    def __init__(
        self,
        origin: httpcore.Origin,
        order: list[str],
        releases: dict[str, anyio.Event],
        *,
        multiplexed: bool = False,
        max_requests: int = 1,
    ) -> None:
        self._origin = origin
        self._order = order
        self._releases = releases
        self._multiplexed = multiplexed
        self._max_requests = max_requests
        self._active_requests = 0
        self._request_count = 0
        self._closed = False

    async def handle_async_request(self, request: httpcore.Request) -> httpcore.Response:
        label = request.url.target.decode().lstrip("/")
        self._active_requests += 1
        self._request_count += 1
        self._order.append(f"allocate:{label}")
        return httpcore.Response(
            200,
            content=AsyncControlledStream(
                self._order,
                label,
                self._releases[label],
                self._release_request,
            ),
        )

    def _release_request(self) -> None:
        self._active_requests -= 1

    async def aclose(self) -> None:
        self._closed = True

    def info(self) -> str:
        return f"controlled:{self._origin}:{self._request_count}"

    def can_handle_request(self, origin: httpcore.Origin) -> bool:
        return origin == self._origin

    def is_available(self) -> bool:
        if self._closed:
            return False
        if self._multiplexed:
            return self._active_requests < self._max_requests
        return self._active_requests == 0

    def has_expired(self) -> bool:
        return self._closed

    def is_idle(self) -> bool:
        return not self._closed and self._active_requests == 0

    def is_closed(self) -> bool:
        return self._closed


class ControlledAsyncConnectionPool(httpcore.AsyncConnectionPool):
    def __init__(
        self,
        order: list[str],
        releases: dict[str, anyio.Event],
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._order = order
        self._releases = releases

    def create_connection(self, origin: httpcore.Origin) -> httpcore.AsyncConnectionInterface:
        return AsyncControlledConnection(origin, self._order, self._releases)


class MultiplexedAsyncConnectionPool(ControlledAsyncConnectionPool):
    def create_connection(self, origin: httpcore.Origin) -> httpcore.AsyncConnectionInterface:
        return AsyncControlledConnection(
            origin,
            self._order,
            self._releases,
            multiplexed=True,
            max_requests=2,
        )


class CreatedCountingAsyncConnectionPool(ControlledAsyncConnectionPool):
    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        super().__init__(*args, **kwargs)
        self.creation_origins: list[str] = []

    def create_connection(self, origin: httpcore.Origin) -> httpcore.AsyncConnectionInterface:
        self.creation_origins.append(str(origin))
        return super().create_connection(origin)


class FailingAsyncConnectionPool(CreatedCountingAsyncConnectionPool):
    def create_connection(self, origin: httpcore.Origin) -> httpcore.AsyncConnectionInterface:
        self.creation_origins.append(str(origin))
        if str(origin) == "http://a.example:80":
            raise httpcore.ConnectError("connection refused")
        return ControlledAsyncConnectionPool.create_connection(self, origin)


def event(events: dict[str, anyio.Event], label: str) -> anyio.Event:
    event = anyio.Event()
    events[label] = event
    return event


@pytest.mark.anyio
async def test_origin_fairness_rotates_connection_opportunities():
    order: list[str] = []
    starts: dict[str, anyio.Event] = {}
    releases: dict[str, anyio.Event] = {}
    pool = ControlledAsyncConnectionPool(
        order,
        releases,
        max_connections=1,
        origin_fairness=True,
    )

    async def request(label: str, origin_host: str) -> None:
        start = event(starts, label)
        release = event(releases, label)
        async with pool.stream("GET", f"http://{origin_host}/{label}"):
            start.set()
            await release.wait()

    async with pool:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(request, "a0", "a.example")
            await anyio.sleep(0)
            await starts["a0"].wait()
            for label in ("a1", "a2", "a3"):
                task_group.start_soon(request, label, "a.example")
            task_group.start_soon(request, "b0", "b.example")
            await anyio.sleep(0.01)

            releases["a0"].set()
            await starts["b0"].wait()
            releases["b0"].set()
            await starts["a1"].wait()
            releases["a1"].set()

            await starts["a2"].wait()
            releases["a2"].set()
            event(releases, "b1")
            task_group.start_soon(request, "b1", "b.example")
            await anyio.sleep(0)
            await starts["b1"].wait()
            releases["b1"].set()
            await starts["a3"].wait()
            releases["a3"].set()

    assert [item for item in order if item.startswith("allocate:")] == [
        "allocate:a0",
        "allocate:b0",
        "allocate:a1",
        "allocate:a2",
        "allocate:b1",
        "allocate:a3",
    ]


@pytest.mark.anyio
async def test_origin_fairness_timeout_does_not_consume_turn():
    order: list[str] = []
    starts: dict[str, anyio.Event] = {}
    releases: dict[str, anyio.Event] = {}
    pool = ControlledAsyncConnectionPool(
        order,
        releases,
        max_connections=1,
        origin_fairness=True,
    )

    async def request(label: str, origin_host: str) -> None:
        start = event(starts, label)
        release = event(releases, label)
        async with pool.stream("GET", f"http://{origin_host}/{label}"):
            start.set()
            await release.wait()

    async with pool:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(request, "a0", "a.example")
            await anyio.sleep(0)
            await starts["a0"].wait()
            with pytest.raises(httpcore.PoolTimeout):
                await pool.request(
                    "GET",
                    "http://b.example/b0",
                    extensions={"timeout": {"pool": 0}},
                )

            task_group.start_soon(request, "c0", "c.example")
            await anyio.sleep(0.01)
            releases["a0"].set()
            await starts["c0"].wait()
            releases["c0"].set()

    assert [item for item in order if item.startswith("allocate:")] == [
        "allocate:a0",
        "allocate:c0",
    ]


@pytest.mark.anyio
async def test_origin_fairness_counts_multiplexed_connections():
    order: list[str] = []
    starts: dict[str, anyio.Event] = {}
    releases: dict[str, anyio.Event] = {}
    pool = MultiplexedAsyncConnectionPool(
        order,
        releases,
        max_connections=1,
        max_connections_per_origin=1,
        origin_fairness=True,
    )

    async def request(label: str, origin_host: str) -> None:
        start = event(starts, label)
        release = event(releases, label)
        async with pool.stream("GET", f"http://{origin_host}/{label}"):
            start.set()
            await release.wait()

    async with pool:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(request, "a0", "a.example")
            await anyio.sleep(0)
            await starts["a0"].wait()
            task_group.start_soon(request, "a1", "a.example")
            await anyio.sleep(0)
            await starts["a1"].wait()
            task_group.start_soon(request, "b0", "b.example")
            await anyio.sleep(0.01)

            releases["a0"].set()
            releases["a1"].set()
            await starts["b0"].wait()
            releases["b0"].set()

    assert [item for item in order if item.startswith("allocate:")] == [
        "allocate:a0",
        "allocate:a1",
        "allocate:b0",
    ]


@pytest.mark.anyio
async def test_origin_fairness_respects_max_connections_per_origin():
    order: list[str] = []
    releases: dict[str, anyio.Event] = {}
    pool = CreatedCountingAsyncConnectionPool(
        order,
        releases,
        max_connections=4,
        origin_fairness=True,
        max_connections_per_origin=1,
    )

    async def request(label: str, origin_host: str) -> None:
        event(releases, label).set()
        await pool.request("GET", f"http://{origin_host}/{label}")

    async with pool:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(request, "a0", "a.example")
            task_group.start_soon(request, "b0", "b.example")
            await anyio.sleep(0.01)
    assert pool.creation_origins == [
        "http://a.example:80",
        "http://b.example:80",
    ]


@pytest.mark.anyio
async def test_origin_fairness_releases_failed_connection_opportunity():
    order: list[str] = []
    releases: dict[str, anyio.Event] = {}
    pool = FailingAsyncConnectionPool(
        order,
        releases,
        max_connections=1,
        origin_fairness=True,
    )

    async with pool:
        with pytest.raises(httpcore.ConnectError):
            await pool.request("GET", "http://a.example/a0")
        assert pool.connections == []

        event(releases, "b0").set()
        response = await pool.request("GET", "http://b.example/b0")
        assert response.status == 200

    assert pool.creation_origins == [
        "http://a.example:80",
        "http://b.example:80",
    ]


@pytest.mark.anyio
async def test_connection_pool_with_keepalive():
    """
    By default HTTP/1.1 requests should be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_keepalive_connections=1
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            )
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>"
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            )
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>"
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )

        # Sending a request to a different origin will not reuse the existing IDLE connection.
        async with pool.stream("GET", "http://example.com/") as response_1, pool.stream(
            "GET", "http://example.com/"
        ) as response_2:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>",
                "<AsyncHTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]
            assert (
                repr(pool)
                == "<AsyncConnectionPool [Requests: 2 active, 0 queued | Connections: 2 active, 1 idle]>"
            )
            await response_1.aread()
            await response_2.aread()

        assert response_1.status == 200
        assert response_1.content == b"Hello, world!"
        assert response_2.status == 200
        assert response_2.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['http://example.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
        ]
        assert (
            repr(pool)
            == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )


@pytest.mark.anyio
async def test_connection_pool_with_close():
    """
    HTTP/1.1 requests that include a 'Connection: Close' header should
    not be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream(
            "GET", "https://example.com/", headers={"Connection": "close"}
        ) as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.anyio
async def test_connection_pool_with_http2():
    """
    Test a connection pool with HTTP/2 requests.
    """
    network_backend = httpcore.AsyncMockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=3,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=3, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
        ],
        http2=True,
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 2]>"
        ]


@pytest.mark.anyio
async def test_connection_pool_with_http2_goaway():
    """
    Test a connection pool with HTTP/2 requests, that cleanly disconnects
    with a GoAway frame after the first request.
    """
    network_backend = httpcore.AsyncMockBackend(
        buffer=[
            hyperframe.frame.SettingsFrame().serialize(),
            hyperframe.frame.HeadersFrame(
                stream_id=1,
                data=hpack.Encoder().encode(
                    [
                        (b":status", b"200"),
                        (b"content-type", b"plain/text"),
                    ]
                ),
                flags=["END_HEADERS"],
            ).serialize(),
            hyperframe.frame.DataFrame(
                stream_id=1, data=b"Hello, world!", flags=["END_STREAM"]
            ).serialize(),
            hyperframe.frame.GoAwayFrame(
                stream_id=0, error_code=0, last_stream_id=1
            ).serialize(),
            b"",
        ],
        http2=True,
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # Sending a second request to the same origin will require a new connection.
        # The original connection has now been closed.
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<AsyncHTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>",
        ]


@pytest.mark.anyio
async def test_trace_request():
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        await pool.request("GET", "https://example.com/", extensions={"trace": trace})

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
        "http11.receive_response_body.started",
        "http11.receive_response_body.complete",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]


@pytest.mark.anyio
async def test_debug_request(caplog):
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    caplog.set_level(logging.DEBUG)

    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        await pool.request("GET", "http://example.com/")

    assert caplog.record_tuples == [
        (
            "httpcore.connection",
            logging.DEBUG,
            "connect_tcp.started host='example.com' port=80 local_address=None timeout=None socket_options=None",
        ),
        (
            "httpcore.connection",
            logging.DEBUG,
            "connect_tcp.complete return_value=<httpcore.AsyncMockStream>",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "send_request_headers.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "send_request_headers.complete"),
        (
            "httpcore.http11",
            logging.DEBUG,
            "send_request_body.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "send_request_body.complete"),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_headers.started request=<Request [b'GET']>",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_headers.complete return_value="
            "(b'HTTP/1.1', 200, b'OK', [(b'Content-Type', b'plain/text'), (b'Content-Length', b'13')])",
        ),
        (
            "httpcore.http11",
            logging.DEBUG,
            "receive_response_body.started request=<Request [b'GET']>",
        ),
        ("httpcore.http11", logging.DEBUG, "receive_response_body.complete"),
        ("httpcore.http11", logging.DEBUG, "response_closed.started"),
        ("httpcore.http11", logging.DEBUG, "response_closed.complete"),
        ("httpcore.connection", logging.DEBUG, "close.started"),
        ("httpcore.connection", logging.DEBUG, "close.complete"),
    ]


@pytest.mark.anyio
async def test_connection_pool_with_http_exception():
    """
    HTTP/1.1 requests that result in an exception during the connection should
    not be returned to the connection pool.
    """
    network_backend = httpcore.AsyncMockBackend([b"Wait, this isn't valid HTTP!"])

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(Exception):
            await pool.request(
                "GET", "https://example.com/", extensions={"trace": trace}
            )

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.failed",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]


@pytest.mark.anyio
async def test_connection_pool_with_connect_exception():
    """
    HTTP/1.1 requests that result in an exception during connection should not
    be returned to the connection pool.
    """

    class FailedConnectBackend(httpcore.AsyncMockBackend):
        async def connect_tcp(
            self,
            host: str,
            port: int,
            timeout: typing.Optional[float] = None,
            local_address: typing.Optional[str] = None,
            socket_options: typing.Optional[
                typing.Iterable[httpcore.SOCKET_OPTION]
            ] = None,
        ) -> httpcore.AsyncNetworkStream:
            raise httpcore.ConnectError("Could not connect")

    network_backend = FailedConnectBackend([])

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(Exception):
            await pool.request(
                "GET", "https://example.com/", extensions={"trace": trace}
            )

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.failed",
    ]


@pytest.mark.anyio
async def test_connection_pool_with_immediate_expiry():
    """
    Connection pools with keepalive_expiry=0.0 should immediately expire
    keep alive connections.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        keepalive_expiry=0.0,
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.anyio
async def test_connection_pool_with_no_keepalive_connections_allowed():
    """
    When 'max_keepalive_connections=0' is used, IDLE connections should not
    be returned to the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        max_keepalive_connections=0, network_backend=network_backend
    ) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        async with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<AsyncHTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []


@pytest.mark.trio
async def test_connection_pool_concurrency():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"http://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "b.com", "c.com", "d.com", "e.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Each connection was to a different host, and only sent a single
            # request on that connection.
            assert item[0] in [
                "<AsyncHTTPConnection ['http://a.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://b.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://c.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://d.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['http://e.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]


@pytest.mark.trio
async def test_connection_pool_concurrency_same_domain_closing():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"Connection: close\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend, http2=True
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Only a single request was sent on each connection.
            assert (
                item[0]
                == "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            )


@pytest.mark.trio
async def test_connection_pool_concurrency_same_domain_keepalive():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
        * 5
    )

    async def fetch(pool, domain, info_list):
        async with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            await response.aread()

    async with httpcore.AsyncConnectionPool(
        max_connections=1, network_backend=network_backend, http2=True
    ) as pool:
        info_list: typing.List[str] = []
        async with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # The connection sent multiple requests.
            assert item[0] in [
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 3]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 4]>",
                "<AsyncHTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 5]>",
            ]

    assert (
        repr(pool)
        == "<AsyncConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 0 idle]>"
    )


@pytest.mark.anyio
async def test_unsupported_protocol():
    async with httpcore.AsyncConnectionPool() as pool:
        with pytest.raises(httpcore.UnsupportedProtocol):
            await pool.request("GET", "ftp://www.example.com/")

        with pytest.raises(httpcore.UnsupportedProtocol):
            await pool.request("GET", "://www.example.com/")


@pytest.mark.anyio
async def test_connection_pool_closed_while_request_in_flight():
    """
    Closing a connection pool while a request/response is still in-flight
    should raise an error.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Send a request, and then close the connection pool while the
        # response has not yet been streamed.
        async with pool.stream("GET", "https://example.com/") as response:
            await pool.aclose()
            with pytest.raises(httpcore.ReadError):
                await response.aread()


@pytest.mark.anyio
async def test_connection_pool_timeout():
    """
    Ensure that exceeding max_connections can cause a request to timeout.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Send a request to a pool that is configured to only support a single
        # connection, and then ensure that a second concurrent request
        # fails with a timeout.
        async with pool.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore.PoolTimeout):
                extensions = {"timeout": {"pool": 0.0001}}
                await pool.request("GET", "https://example.com/", extensions=extensions)


@pytest.mark.anyio
async def test_connection_pool_timeout_zero():
    """
    A pool timeout of 0 shouldn't raise a PoolTimeout if there's
    no need to wait on a new connection.
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    # Use a pool timeout of zero.
    extensions = {"timeout": {"pool": 0}}

    # A connection pool configured to allow only one connection at a time.
    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Two consecutive requests with a pool timeout of zero.
        # Both succeed without raising a timeout.
        response = await pool.request(
            "GET", "https://example.com/", extensions=extensions
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        response = await pool.request(
            "GET", "https://example.com/", extensions=extensions
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # A connection pool configured to allow only one connection at a time.
    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Two concurrent requests with a pool timeout of zero.
        # Only the first will succeed without raising a timeout.
        async with pool.stream(
            "GET", "https://example.com/", extensions=extensions
        ) as response:
            # The first response hasn't yet completed.
            with pytest.raises(httpcore.PoolTimeout):
                # So a pool timeout occurs.
                await pool.request("GET", "https://example.com/", extensions=extensions)
            # The first response now completes.
            await response.aread()

        assert response.status == 200
        assert response.content == b"Hello, world!"


@pytest.mark.anyio
async def test_http11_upgrade_connection():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    network_backend = httpcore.AsyncMockBackend(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )

    called = []

    async def trace(name, kwargs):
        called.append(name)

    async with httpcore.AsyncConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        async with pool.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
            extensions={"trace": trace},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = await network_stream.read(max_bytes=1024)
            assert content == b"..."

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.complete",
        "connection.start_tls.started",
        "connection.start_tls.complete",
        "http11.send_request_headers.started",
        "http11.send_request_headers.complete",
        "http11.send_request_body.started",
        "http11.send_request_body.complete",
        "http11.receive_response_headers.started",
        "http11.receive_response_headers.complete",
        "http11.response_closed.started",
        "http11.response_closed.complete",
    ]
