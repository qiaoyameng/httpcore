import logging
import threading
import typing

import hpack
import hyperframe.frame
import pytest
from tests import concurrency

import httpcore


class ControlledStream:
    def __init__(
        self,
        order: list[str],
        label: str,
        started: threading.Event,
        release: threading.Event,
        on_close: typing.Callable[[], None],
    ) -> None:
        self._order = order
        self._label = label
        self._started = started
        self._release = release
        self._on_close = on_close

    def __iter__(self) -> typing.Iterator[bytes]:
        self._started.set()
        self._release.wait()
        yield b""
        self._on_close()
        self._order.append(f"release:{self._label}")


class ControlledConnection(httpcore.ConnectionInterface):
    def __init__(
        self,
        origin: httpcore.Origin,
        order: list[str],
        events: dict[str, tuple[threading.Event, threading.Event]],
        *,
        multiplexed: bool = False,
        max_requests: int = 1,
    ) -> None:
        self._origin = origin
        self._order = order
        self._events = events
        self._multiplexed = multiplexed
        self._max_requests = max_requests
        self._active_requests = 0
        self._request_count = 0
        self._closed = False

    def handle_request(self, request: httpcore.Request) -> httpcore.Response:
        label = request.url.target.decode().lstrip("/")
        self._active_requests += 1
        self._request_count += 1
        self._order.append(f"allocate:{label}")
        started, release = self._events[label]
        return httpcore.Response(
            200,
            content=ControlledStream(
                self._order,
                label,
                started,
                release,
                self._release_request,
            ),
        )

    def _release_request(self) -> None:
        self._active_requests -= 1

    def close(self) -> None:
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


class ControlledConnectionPool(httpcore.ConnectionPool):
    def __init__(
        self,
        order: list[str],
        events: dict[str, tuple[threading.Event, threading.Event]],
        **kwargs: typing.Any,
    ) -> None:
        super().__init__(**kwargs)
        self._order = order
        self._events = events

    def create_connection(self, origin: httpcore.Origin) -> httpcore.ConnectionInterface:
        return ControlledConnection(origin, self._order, self._events)

    def event(self, label: str, event_index: int) -> threading.Event:
        return self._events[label][event_index]


class MultiplexedControlledConnectionPool(ControlledConnectionPool):
    def create_connection(self, origin: httpcore.Origin) -> httpcore.ConnectionInterface:
        return ControlledConnection(
            origin,
            self._order,
            self._events,
            multiplexed=True,
            max_requests=2,
        )



class CreatedCountingConnectionPool(ControlledConnectionPool):
    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        super().__init__(*args, **kwargs)
        self.creation_origins: list[str] = []

    def create_connection(self, origin: httpcore.Origin) -> httpcore.ConnectionInterface:
        self.creation_origins.append(str(origin))
        return ControlledConnectionPool.create_connection(self, origin)


class FailingConnectionPool(CreatedCountingConnectionPool):
    def create_connection(self, origin: httpcore.Origin) -> httpcore.ConnectionInterface:
        self.creation_origins.append(str(origin))
        if str(origin) == "http://a.example:80":
            raise httpcore.ConnectError("connection refused")
        return ControlledConnectionPool.create_connection(self, origin)


def event_pair(
    events: dict[str, tuple[threading.Event, threading.Event]], label: str
) -> tuple[threading.Event, threading.Event]:
    pair = (threading.Event(), threading.Event())
    events[label] = pair
    return pair


def start_request(
    pool: ControlledConnectionPool,
    events: dict[str, tuple[threading.Event, threading.Event]],
    label: str,
    origin_host: str,
) -> threading.Thread:
    event_pair(events, label)
    thread = threading.Thread(
        target=pool.request, args=("GET", f"http://{origin_host}/{label}")
    )
    thread.start()
    return thread


def test_origin_fairness_rotates_connection_opportunities():
    order: list[str] = []
    events: dict[str, tuple[threading.Event, threading.Event]] = {}
    pool = ControlledConnectionPool(
        order,
        events,
        max_connections=1,
        origin_fairness=True,
    )

    with pool:
        threads = {
            "a0": start_request(pool, events, "a0", "a.example"),
        }
        pool.event("a0", 0).wait()
        for label in ("a1", "a2", "a3"):
            threads[label] = start_request(pool, events, label, "a.example")
        threads["b0"] = start_request(pool, events, "b0", "b.example")

        pool.event("a0", 1).set()
        pool.event("b0", 0).wait()
        pool.event("b0", 1).set()
        pool.event("a1", 0).wait()

        pool.event("a1", 1).set()
        pool.event("a2", 0).wait()
        pool.event("a2", 1).set()
        threads["b1"] = start_request(pool, events, "b1", "b.example")
        pool.event("b1", 0).wait()
        pool.event("b1", 1).set()
        pool.event("a3", 0).wait()
        pool.event("a3", 1).set()

        for thread in threads.values():
            thread.join()

    assert [item for item in order if item.startswith("allocate:")] == [
        "allocate:a0",
        "allocate:b0",
        "allocate:a1",
        "allocate:a2",
        "allocate:b1",
        "allocate:a3",
    ]


def test_origin_fairness_timeout_does_not_consume_turn():
    order: list[str] = []
    events: dict[str, tuple[threading.Event, threading.Event]] = {}
    pool = ControlledConnectionPool(
        order,
        events,
        max_connections=1,
        origin_fairness=True,
    )

    with pool:
        first = start_request(pool, events, "a0", "a.example")
        pool.event("a0", 0).wait()

        with pytest.raises(httpcore.PoolTimeout):
            pool.request(
                "GET",
                "http://b.example/b0",
                extensions={"timeout": {"pool": 0}},
            )

        c0_started = threading.Event()
        c0_release = threading.Event()
        events["c0"] = (c0_started, c0_release)
        second = threading.Thread(
            target=pool.request, args=("GET", "http://c.example/c0")
        )
        second.start()
        pool.event("a0", 1).set()
        first.join()
        c0_started.wait()
        c0_release.set()
        second.join()

    assert [item for item in order if item.startswith("allocate:")] == [
        "allocate:a0",
        "allocate:c0",
    ]


def test_origin_fairness_counts_multiplexed_connections():
    order: list[str] = []
    events: dict[str, tuple[threading.Event, threading.Event]] = {}
    pool = MultiplexedControlledConnectionPool(
        order,
        events,
        max_connections=1,
        max_connections_per_origin=1,
        origin_fairness=True,
    )

    with pool:
        first = start_request(pool, events, "a0", "a.example")
        pool.event("a0", 0).wait()
        a1 = start_request(pool, events, "a1", "a.example")
        pool.event("a1", 0).wait()
        b0 = start_request(pool, events, "b0", "b.example")

        pool.event("a0", 1).set()
        pool.event("a1", 1).set()
        pool.event("b0", 0).wait()
        pool.event("b0", 1).set()

        first.join()
        a1.join()
        b0.join()

    assert [item for item in order if item.startswith("allocate:")] == [
        "allocate:a0",
        "allocate:a1",
        "allocate:b0",
    ]


def test_origin_fairness_respects_max_connections_per_origin():
    order: list[str] = []
    events: dict[str, tuple[threading.Event, threading.Event]] = {}
    pool = CreatedCountingConnectionPool(
        order,
        events,
        max_connections=4,
        origin_fairness=True,
        max_connections_per_origin=1,
    )

    for label, origin in (("a0", "a.example"), ("b0", "b.example")):
        event_pair(events, label)[1].set()

    with pool:
        first = threading.Thread(
            target=pool.request, args=("GET", "http://a.example/a0")
        )
        second = threading.Thread(
            target=pool.request, args=("GET", "http://b.example/b0")
        )
        first.start()
        second.start()
        first.join()
        second.join()

    assert pool.creation_origins == [
        "http://a.example:80",
        "http://b.example:80",
    ]


def test_origin_fairness_releases_failed_connection_opportunity():
    order: list[str] = []
    events: dict[str, tuple[threading.Event, threading.Event]] = {}
    pool = FailingConnectionPool(
        order,
        events,
        max_connections=1,
        origin_fairness=True,
    )

    with pool:
        with pytest.raises(httpcore.ConnectError):
            pool.request("GET", "http://a.example/a0")
        assert pool.connections == []

        event_pair(events, "b0")[1].set()
        response = pool.request("GET", "http://b.example/b0")
        assert response.status == 200

    assert pool.creation_origins == [
        "http://a.example:80",
        "http://b.example:80",
    ]


def test_connection_pool_with_keepalive():
    """
    By default HTTP/1.1 requests should be returned to the connection pool.
    """
    network_backend = httpcore.MockBackend(
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

    with httpcore.ConnectionPool(
        network_backend=network_backend, max_keepalive_connections=1
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            assert (
                repr(pool)
                == "<ConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            )
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 1]>"
        ]
        assert (
            repr(pool)
            == "<ConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>"
            ]
            assert (
                repr(pool)
                == "<ConnectionPool [Requests: 1 active, 0 queued | Connections: 1 active, 0 idle]>"
            )
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>"
        ]
        assert (
            repr(pool)
            == "<ConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )

        # Sending a request to a different origin will not reuse the existing IDLE connection.
        with pool.stream("GET", "http://example.com/") as response_1, pool.stream(
            "GET", "http://example.com/"
        ) as response_2:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<HTTPConnection ['https://example.com:443', HTTP/1.1, IDLE, Request Count: 2]>",
                "<HTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://example.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]
            assert (
                repr(pool)
                == "<ConnectionPool [Requests: 2 active, 0 queued | Connections: 2 active, 1 idle]>"
            )
            response_1.read()
            response_2.read()

        assert response_1.status == 200
        assert response_1.content == b"Hello, world!"
        assert response_2.status == 200
        assert response_2.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['http://example.com:80', HTTP/1.1, IDLE, Request Count: 1]>",
        ]
        assert (
            repr(pool)
            == "<ConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 1 idle]>"
        )



def test_connection_pool_with_close():
    """
    HTTP/1.1 requests that include a 'Connection: Close' header should
    not be returned to the connection pool.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(network_backend=network_backend) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        with pool.stream(
            "GET", "https://example.com/", headers={"Connection": "close"}
        ) as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []



def test_connection_pool_with_http2():
    """
    Test a connection pool with HTTP/2 requests.
    """
    network_backend = httpcore.MockBackend(
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

    with httpcore.ConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        response = pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # Sending a second request to the same origin will reuse the existing IDLE connection.
        response = pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 2]>"
        ]



def test_connection_pool_with_http2_goaway():
    """
    Test a connection pool with HTTP/2 requests, that cleanly disconnects
    with a GoAway frame after the first request.
    """
    network_backend = httpcore.MockBackend(
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

    with httpcore.ConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will return to the pool, IDLE.
        response = pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>"
        ]

        # Sending a second request to the same origin will require a new connection.
        # The original connection has now been closed.
        response = pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        info = [repr(c) for c in pool.connections]
        assert info == [
            "<HTTPConnection ['https://example.com:443', HTTP/2, IDLE, Request Count: 1]>",
        ]



def test_trace_request():
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    called = []

    def trace(name, kwargs):
        called.append(name)

    with httpcore.ConnectionPool(network_backend=network_backend) as pool:
        pool.request("GET", "https://example.com/", extensions={"trace": trace})

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



def test_debug_request(caplog):
    """
    The 'trace' request extension allows for a callback function to inspect the
    internal events that occur while sending a request.
    """
    caplog.set_level(logging.DEBUG)

    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(network_backend=network_backend) as pool:
        pool.request("GET", "http://example.com/")

    assert caplog.record_tuples == [
        (
            "httpcore.connection",
            logging.DEBUG,
            "connect_tcp.started host='example.com' port=80 local_address=None timeout=None socket_options=None",
        ),
        (
            "httpcore.connection",
            logging.DEBUG,
            "connect_tcp.complete return_value=<httpcore.MockStream>",
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



def test_connection_pool_with_http_exception():
    """
    HTTP/1.1 requests that result in an exception during the connection should
    not be returned to the connection pool.
    """
    network_backend = httpcore.MockBackend([b"Wait, this isn't valid HTTP!"])

    called = []

    def trace(name, kwargs):
        called.append(name)

    with httpcore.ConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(Exception):
            pool.request(
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



def test_connection_pool_with_connect_exception():
    """
    HTTP/1.1 requests that result in an exception during connection should not
    be returned to the connection pool.
    """

    class FailedConnectBackend(httpcore.MockBackend):
        def connect_tcp(
            self,
            host: str,
            port: int,
            timeout: typing.Optional[float] = None,
            local_address: typing.Optional[str] = None,
            socket_options: typing.Optional[
                typing.Iterable[httpcore.SOCKET_OPTION]
            ] = None,
        ) -> httpcore.NetworkStream:
            raise httpcore.ConnectError("Could not connect")

    network_backend = FailedConnectBackend([])

    called = []

    def trace(name, kwargs):
        called.append(name)

    with httpcore.ConnectionPool(network_backend=network_backend) as pool:
        # Sending an initial request, which once complete will not return to the pool.
        with pytest.raises(Exception):
            pool.request(
                "GET", "https://example.com/", extensions={"trace": trace}
            )

        info = [repr(c) for c in pool.connections]
        assert info == []

    assert called == [
        "connection.connect_tcp.started",
        "connection.connect_tcp.failed",
    ]



def test_connection_pool_with_immediate_expiry():
    """
    Connection pools with keepalive_expiry=0.0 should immediately expire
    keep alive connections.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(
        keepalive_expiry=0.0,
        network_backend=network_backend,
    ) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []



def test_connection_pool_with_no_keepalive_connections_allowed():
    """
    When 'max_keepalive_connections=0' is used, IDLE connections should not
    be returned to the pool.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(
        max_keepalive_connections=0, network_backend=network_backend
    ) as pool:
        # Sending an intial request, which once complete will not return to the pool.
        with pool.stream("GET", "https://example.com/") as response:
            info = [repr(c) for c in pool.connections]
            assert info == [
                "<HTTPConnection ['https://example.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            ]
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"
        info = [repr(c) for c in pool.connections]
        assert info == []



def test_connection_pool_concurrency():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    def fetch(pool, domain, info_list):
        with pool.stream("GET", f"http://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            response.read()

    with httpcore.ConnectionPool(
        max_connections=1, network_backend=network_backend
    ) as pool:
        info_list: typing.List[str] = []
        with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "b.com", "c.com", "d.com", "e.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Each connection was to a different host, and only sent a single
            # request on that connection.
            assert item[0] in [
                "<HTTPConnection ['http://a.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://b.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://c.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://d.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['http://e.com:80', HTTP/1.1, ACTIVE, Request Count: 1]>",
            ]



def test_connection_pool_concurrency_same_domain_closing():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"Connection: close\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    def fetch(pool, domain, info_list):
        with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            response.read()

    with httpcore.ConnectionPool(
        max_connections=1, network_backend=network_backend, http2=True
    ) as pool:
        info_list: typing.List[str] = []
        with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # Only a single request was sent on each connection.
            assert (
                item[0]
                == "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>"
            )



def test_connection_pool_concurrency_same_domain_keepalive():
    """
    HTTP/1.1 requests made in concurrency must not ever exceed the maximum number
    of allowable connection in the pool.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
        * 5
    )

    def fetch(pool, domain, info_list):
        with pool.stream("GET", f"https://{domain}/") as response:
            info = [repr(c) for c in pool.connections]
            info_list.append(info)
            response.read()

    with httpcore.ConnectionPool(
        max_connections=1, network_backend=network_backend, http2=True
    ) as pool:
        info_list: typing.List[str] = []
        with concurrency.open_nursery() as nursery:
            for domain in ["a.com", "a.com", "a.com", "a.com", "a.com"]:
                nursery.start_soon(fetch, pool, domain, info_list)

        for item in info_list:
            # Check that each time we inspected the connection pool, only a
            # single connection was established at any one time.
            assert len(item) == 1
            # The connection sent multiple requests.
            assert item[0] in [
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 1]>",
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 2]>",
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 3]>",
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 4]>",
                "<HTTPConnection ['https://a.com:443', HTTP/1.1, ACTIVE, Request Count: 5]>",
            ]

    assert (
        repr(pool)
        == "<ConnectionPool [Requests: 0 active, 0 queued | Connections: 0 active, 0 idle]>"
    )



def test_unsupported_protocol():
    with httpcore.ConnectionPool() as pool:
        with pytest.raises(httpcore.UnsupportedProtocol):
            pool.request("GET", "ftp://www.example.com/")

        with pytest.raises(httpcore.UnsupportedProtocol):
            pool.request("GET", "://www.example.com/")



def test_connection_pool_closed_while_request_in_flight():
    """
    Closing a connection pool while a request/response is still in-flight
    should raise an error.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(
        network_backend=network_backend,
    ) as pool:
        # Send a request, and then close the connection pool while the
        # response has not yet been streamed.
        with pool.stream("GET", "https://example.com/") as response:
            pool.close()
            with pytest.raises(httpcore.ReadError):
                response.read()



def test_connection_pool_timeout():
    """
    Ensure that exceeding max_connections can cause a request to timeout.
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )

    with httpcore.ConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Send a request to a pool that is configured to only support a single
        # connection, and then ensure that a second concurrent request
        # fails with a timeout.
        with pool.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore.PoolTimeout):
                extensions = {"timeout": {"pool": 0.0001}}
                pool.request("GET", "https://example.com/", extensions=extensions)



def test_connection_pool_timeout_zero():
    """
    A pool timeout of 0 shouldn't raise a PoolTimeout if there's
    no need to wait on a new connection.
    """
    network_backend = httpcore.MockBackend(
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
    with httpcore.ConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Two consecutive requests with a pool timeout of zero.
        # Both succeed without raising a timeout.
        response = pool.request(
            "GET", "https://example.com/", extensions=extensions
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

        response = pool.request(
            "GET", "https://example.com/", extensions=extensions
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # A connection pool configured to allow only one connection at a time.
    with httpcore.ConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        # Two concurrent requests with a pool timeout of zero.
        # Only the first will succeed without raising a timeout.
        with pool.stream(
            "GET", "https://example.com/", extensions=extensions
        ) as response:
            # The first response hasn't yet completed.
            with pytest.raises(httpcore.PoolTimeout):
                # So a pool timeout occurs.
                pool.request("GET", "https://example.com/", extensions=extensions)
            # The first response now completes.
            response.read()

        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_http11_upgrade_connection():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    network_backend = httpcore.MockBackend(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )

    called = []

    def trace(name, kwargs):
        called.append(name)

    with httpcore.ConnectionPool(
        network_backend=network_backend, max_connections=1
    ) as pool:
        with pool.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
            extensions={"trace": trace},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = network_stream.read(max_bytes=1024)
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
