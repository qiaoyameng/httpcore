"""
Tests for HTTP/2 GOAWAY handling.

These tests use an in-memory HTTP/2 peer that serves a script of
serialized frames, so that GOAWAY and response frames can be interleaved
arbitrarily. The bytes written to each connection are recorded, and
replayed through a server-side HTTP/2 state machine, so that tests can
verify exactly how many times each request body was actually sent.
"""

import typing

import h2.config
import h2.connection
import h2.events
import hpack
import hyperframe.frame
import pytest

import httpcore


class MockSSLObject:
    def selected_alpn_protocol(self) -> str:
        return "h2"


class ScriptedHTTP2Stream(httpcore.AsyncNetworkStream):
    """
    An in-memory HTTP/2 peer. Outgoing bytes are recorded, and incoming
    bytes are served from a script of serialized frames.
    """

    def __init__(self, script: typing.List[bytes]) -> None:
        self._script = list(script)
        self.written: typing.List[bytes] = []
        self._closed = False

    async def write(
        self, buffer: bytes, timeout: typing.Optional[float] = None
    ) -> None:
        self.written.append(buffer)

    async def read(
        self, max_bytes: int, timeout: typing.Optional[float] = None
    ) -> bytes:
        if self._closed:
            raise httpcore.ReadError("Connection closed")
        if not self._script:
            return b""
        return self._script.pop(0)

    async def aclose(self) -> None:
        self._closed = True

    async def start_tls(
        self,
        ssl_context: typing.Any,
        server_hostname: typing.Optional[str] = None,
        timeout: typing.Optional[float] = None,
    ) -> "ScriptedHTTP2Stream":
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        return MockSSLObject() if info == "ssl_object" else None


class ScriptedHTTP2Backend(httpcore.AsyncNetworkBackend):
    """
    Hands out a fresh scripted HTTP/2 peer for each new connection.
    """

    def __init__(self, scripts: typing.List[typing.List[bytes]]) -> None:
        self._scripts = scripts
        self.streams: typing.List[ScriptedHTTP2Stream] = []

    def _connect(self) -> ScriptedHTTP2Stream:
        stream = ScriptedHTTP2Stream(self._scripts[len(self.streams)])
        self.streams.append(stream)
        return stream

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: typing.Optional[float] = None,
        local_address: typing.Optional[str] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> ScriptedHTTP2Stream:
        return self._connect()

    async def connect_unix_socket(
        self,
        path: str,
        timeout: typing.Optional[float] = None,
        socket_options: typing.Optional[typing.Iterable[httpcore.SOCKET_OPTION]] = None,
    ) -> ScriptedHTTP2Stream:
        return self._connect()

    async def sleep(self, seconds: float) -> None:
        pass


def settings_frame() -> bytes:
    return hyperframe.frame.SettingsFrame(
        settings={h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 100}
    ).serialize()


def headers_frame(stream_id: int) -> bytes:
    return hyperframe.frame.HeadersFrame(
        stream_id=stream_id,
        data=hpack.Encoder().encode([(b":status", b"200")]),
        flags=["END_HEADERS"],
    ).serialize()


def data_frame(stream_id: int, data: bytes) -> bytes:
    return hyperframe.frame.DataFrame(
        stream_id=stream_id, data=data, flags=["END_STREAM"]
    ).serialize()


def goaway_frame(last_stream_id: int) -> bytes:
    return hyperframe.frame.GoAwayFrame(
        stream_id=0, error_code=0, last_stream_id=last_stream_id
    ).serialize()


def request_bodies_received(written: typing.List[bytes]) -> typing.List[bytes]:
    """
    Replay the bytes written to a connection through a server-side HTTP/2
    state machine, returning the request body received on each stream.
    """
    peer = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=False)
    )
    peer.initiate_connection()
    peer.update_settings({h2.settings.SettingCodes.INITIAL_WINDOW_SIZE: 2**24})
    peer.increment_flow_control_window(2**24)
    bodies: typing.Dict[int, bytes] = {}
    events = peer.receive_data(b"".join(written))
    for event in events:
        if isinstance(event, h2.events.RequestReceived):
            bodies[event.stream_id] = b""
        elif isinstance(event, h2.events.DataReceived):
            bodies[event.stream_id] += event.data
            peer.acknowledge_received_data(
                event.flow_controlled_length, event.stream_id
            )
    return [bodies[stream_id] for stream_id in sorted(bodies)]


@pytest.mark.anyio
async def test_http2_goaway_replays_request_on_new_connection():
    """
    A request interrupted by a GOAWAY frame before the server processed it
    is replayed onto a new connection, if 'allow_replay' is set.
    """
    backend = ScriptedHTTP2Backend(
        scripts=[
            [settings_frame(), goaway_frame(last_stream_id=0)],
            [
                settings_frame(),
                headers_frame(stream_id=1),
                data_frame(stream_id=1, data=b"Hello, world!"),
            ],
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        response = await pool.request(
            "POST",
            "https://example.com/",
            headers={b"content-length": b"18"},
            content=b'{"data": "upload"}',
            extensions={"allow_replay": True},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # The request body was sent exactly once on each connection.
    assert len(backend.streams) == 2
    assert request_bodies_received(backend.streams[0].written) == [
        b'{"data": "upload"}'
    ]
    assert request_bodies_received(backend.streams[1].written) == [
        b'{"data": "upload"}'
    ]


@pytest.mark.anyio
async def test_http2_goaway_inflight_stream_completes():
    """
    Streams at or below the GOAWAY 'last_stream_id' continue to completion,
    while streams above it are moved onto a new connection.
    """
    backend = ScriptedHTTP2Backend(
        scripts=[
            [
                settings_frame(),
                headers_frame(stream_id=1),
                # The GOAWAY frame is interleaved with the response frames.
                goaway_frame(last_stream_id=1),
                data_frame(stream_id=1, data=b"Hello, world!"),
            ],
            [
                settings_frame(),
                headers_frame(stream_id=1),
                data_frame(stream_id=1, data=b"Replayed response."),
            ],
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        async with pool.stream("GET", "https://example.com/inflight") as inflight:
            assert inflight.status == 200
            async with pool.stream(
                "POST",
                "https://example.com/replayed",
                headers={b"content-length": b"18"},
                content=b'{"data": "upload"}',
                extensions={"allow_replay": True},
            ) as replayed:
                assert replayed.status == 200
                assert await replayed.aread() == b"Replayed response."
            # The in-flight response completes on the original connection,
            # even though the GOAWAY frame was received in the meantime.
            assert await inflight.aread() == b"Hello, world!"

    # The GET request was sent once, on the first connection.
    # The POST body was sent once on each connection.
    assert len(backend.streams) == 2
    assert request_bodies_received(backend.streams[0].written) == [
        b"",
        b'{"data": "upload"}',
    ]
    assert request_bodies_received(backend.streams[1].written) == [
        b'{"data": "upload"}'
    ]


@pytest.mark.anyio
async def test_http2_goaway_unsent_request_uses_new_connection():
    """
    A request that has not had its body consumed is moved to a new
    connection without requiring the 'allow_replay' extension.
    """
    backend = ScriptedHTTP2Backend(
        scripts=[
            [settings_frame(), goaway_frame(last_stream_id=0)],
            [
                settings_frame(),
                headers_frame(stream_id=1),
                data_frame(stream_id=1, data=b"Hello, world!"),
            ],
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

    assert len(backend.streams) == 2
    assert request_bodies_received(backend.streams[0].written) == [b""]
    assert request_bodies_received(backend.streams[1].written) == [b""]


@pytest.mark.anyio
async def test_http2_goaway_with_shrinking_last_stream_id():
    """
    A server may send multiple GOAWAY frames with progressively smaller
    'last_stream_id' values. Streams that become excluded as a result are
    moved onto a new connection.
    """
    backend = ScriptedHTTP2Backend(
        scripts=[
            [
                settings_frame(),
                headers_frame(stream_id=1),
                data_frame(stream_id=1, data=b"Hello, world!"),
                goaway_frame(last_stream_id=3),
                goaway_frame(last_stream_id=1),
            ],
            [
                settings_frame(),
                headers_frame(stream_id=1),
                data_frame(stream_id=1, data=b"Replayed response."),
            ],
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        response = await pool.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        response = await pool.request(
            "POST",
            "https://example.com/",
            headers={b"content-length": b"18"},
            content=b'{"data": "upload"}',
            extensions={"allow_replay": True},
        )
        assert response.status == 200
        assert response.content == b"Replayed response."

    assert len(backend.streams) == 2
    assert request_bodies_received(backend.streams[1].written) == [
        b'{"data": "upload"}'
    ]


@pytest.mark.anyio
async def test_http2_goaway_with_increasing_last_stream_id_is_an_error():
    """
    Servers MUST NOT increase 'last_stream_id' in subsequent GOAWAY frames.
    This is treated as a protocol error, with a finite, diagnosable failure.
    """
    backend = ScriptedHTTP2Backend(
        scripts=[
            [
                settings_frame(),
                goaway_frame(last_stream_id=1),
                goaway_frame(last_stream_id=3),
            ],
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        with pytest.raises(httpcore.RemoteProtocolError):
            await pool.request("GET", "https://example.com/")


@pytest.mark.anyio
async def test_http2_goaway_draining_connection_rejects_new_requests():
    """
    Once a GOAWAY frame has been received, new requests are not sent on
    the draining connection, while in-flight streams continue to completion.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ScriptedHTTP2Stream(
        [
            settings_frame(),
            headers_frame(stream_id=1),
            goaway_frame(last_stream_id=3),
            data_frame(stream_id=1, data=b"Hello, world!"),
            headers_frame(stream_id=3),
            data_frame(stream_id=3, data=b"Second response."),
        ]
    )
    async with httpcore.AsyncHTTP2Connection(origin=origin, stream=stream) as conn:
        async with conn.stream("GET", "https://example.com/1") as response_1:
            # The second request reads the GOAWAY frame while waiting for
            # its response headers. Both streams are within the processed
            # range, and continue to completion.
            async with conn.stream("GET", "https://example.com/2") as response_2:
                assert not conn.is_available()

                # New requests are rejected without anything being sent.
                writes = len(stream.written)
                with pytest.raises(httpcore.ConnectionNotAvailable):
                    await conn.request("GET", "https://example.com/3")
                assert len(stream.written) == writes

                assert await response_2.aread() == b"Second response."
            assert await response_1.aread() == b"Hello, world!"


@pytest.mark.anyio
async def test_http2_goaway_replays_are_bounded():
    """
    If every connection refuses the request with a GOAWAY frame, then
    replaying eventually gives up with a diagnosable error.
    """
    backend = ScriptedHTTP2Backend(
        scripts=[
            [settings_frame(), goaway_frame(last_stream_id=0)],
            [settings_frame(), goaway_frame(last_stream_id=0)],
            [settings_frame(), goaway_frame(last_stream_id=0)],
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        with pytest.raises(httpcore.RemoteProtocolError):
            await pool.request(
                "POST",
                "https://example.com/",
                headers={b"content-length": b"18"},
                content=b'{"data": "upload"}',
                extensions={"allow_replay": True},
            )

    # The request was attempted on three connections: the initial attempt
    # plus two replays. The body was sent exactly once on each.
    assert len(backend.streams) == 3
    for stream in backend.streams:
        assert request_bodies_received(stream.written) == [b'{"data": "upload"}']


@pytest.mark.anyio
async def test_http2_goaway_replay_requires_allow_replay():
    """
    Requests are not replayed unless the caller explicitly allows it via
    the 'allow_replay' extension, even if the body could be regenerated.
    """
    backend = ScriptedHTTP2Backend(
        scripts=[
            [settings_frame(), goaway_frame(last_stream_id=0)],
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        with pytest.raises(httpcore.RequestNotReplayable):
            await pool.request(
                "POST",
                "https://example.com/",
                headers={b"content-length": b"18"},
                content=b'{"data": "upload"}',
            )

    # The body was sent exactly once, and no replay was attempted.
    assert len(backend.streams) == 1
    assert request_bodies_received(backend.streams[0].written) == [
        b'{"data": "upload"}'
    ]


@pytest.mark.anyio
async def test_http2_goaway_with_one_shot_request_body():
    """
    A request with a one-shot body that has already been consumed cannot
    be replayed, and fails with a diagnosable error.
    """
    backend = ScriptedHTTP2Backend(
        scripts=[
            [settings_frame(), goaway_frame(last_stream_id=0)],
        ]
    )

    body_sent = []

    async def request_body() -> typing.AsyncIterator[bytes]:
        for chunk in [b'{"data": ', b'"upload"}']:
            body_sent.append(chunk)
            yield chunk

    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        with pytest.raises(httpcore.RequestNotReplayable):
            await pool.request(
                "POST",
                "https://example.com/",
                headers={b"content-length": b"18"},
                content=request_body(),
                extensions={"allow_replay": True},
            )

    # The body was consumed exactly once, and no replay was attempted.
    assert body_sent == [b'{"data": ', b'"upload"}']
    assert len(backend.streams) == 1
    assert request_bodies_received(backend.streams[0].written) == [
        b'{"data": "upload"}'
    ]


@pytest.mark.anyio
async def test_http2_goaway_while_waiting_for_flow_control():
    """
    A GOAWAY frame received while the request body is blocked on flow
    control also results in the request being replayed.
    """
    body = b"x" * 70_000  # Exceeds the initial 65,535 byte flow window.
    backend = ScriptedHTTP2Backend(
        scripts=[
            [goaway_frame(last_stream_id=0)],
            [
                settings_frame(),
                hyperframe.frame.WindowUpdateFrame(
                    stream_id=0, window_increment=70_000
                ).serialize(),
                hyperframe.frame.WindowUpdateFrame(
                    stream_id=1, window_increment=70_000
                ).serialize(),
                headers_frame(stream_id=1),
                data_frame(stream_id=1, data=b"Hello, world!"),
            ],
        ]
    )
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        response = await pool.request(
            "POST",
            "https://example.com/",
            headers={b"content-length": b"70000"},
            content=body,
            extensions={"allow_replay": True},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"

    # The body was partially sent on the first connection, and was then
    # replayed in full on the second connection.
    assert len(backend.streams) == 2
    assert request_bodies_received(backend.streams[0].written) == [body[:65_535]]
    assert request_bodies_received(backend.streams[1].written) == [body]
