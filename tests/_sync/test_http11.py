import time
import typing

import pytest

import httpcore


class ExpectContinueRequestBody:
    def __init__(self) -> None:
        self.read_count = 0
        self.first_read_at: float | None = None
        self.closed = False
        self._sent = False

    def __iter__(self) -> typing.Iterator[bytes]:
        return self

    def __next__(self) -> bytes:
        if self._sent:
            raise StopIteration
        self.first_read_at = time.monotonic()
        self.read_count += 1
        self._sent = True
        return b"Hello, world!"

    def close(self) -> None:
        self.closed = True


class OuterExpectContinueRequestBody:
    def __init__(self) -> None:
        self.read_count = 0
        self.first_read_at: float | None = None
        self.closed = False

    def __iter__(self) -> typing.Iterator[bytes]:
        outer = self

        def iterate() -> typing.Iterator[bytes]:
            outer.first_read_at = time.monotonic()
            outer.read_count += 1
            yield b"Hello, world!"

        return iterate()

    def close(self) -> None:
        self.closed = True


class ExpectContinueStream(httpcore.NetworkStream):
    def __init__(
        self,
        buffer: list[bytes],
        *,
        read_timeouts: list[float | None] | None = None,
        timeout_reads: set[int] | None = None,
        slow_reads: set[int] | None = None,
    ) -> None:
        self._buffer = buffer
        self._read_timeouts = read_timeouts
        self._timeout_reads = timeout_reads or set()
        self._slow_reads = slow_reads or set()
        self._read_count = 0
        self.written = b""
        self.closed = False

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        self._read_count += 1
        if self._read_count in self._slow_reads:
            time.sleep(0.01)
        if self._read_timeouts is not None:
            self._read_timeouts.append(timeout)
        if self._read_count in self._timeout_reads:
            raise httpcore.ReadTimeout
        if not self._buffer:
            return b""
        return self._buffer.pop(0)

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self.written += buffer

    def close(self) -> None:
        self.closed = True


class RequestBodyWriteFailureStream(httpcore.NetworkStream):
    def __init__(self) -> None:
        self.closed = False
        self._buffer = [b"HTTP/1.1 100 Continue\r\n\r\n"]

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if not self._buffer:
            return b""
        return self._buffer.pop(0)

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if b"Hello, world!" in buffer:
            raise httpcore.WriteError

    def close(self) -> None:
        self.closed = True



def test_http11_connection():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b"Hello, world!"

        assert conn.is_idle()
        assert not conn.is_closed()
        assert conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', IDLE, Request Count: 1]>"
        )



def test_http11_connection_unread_response():
    """
    If the client releases the response without reading it to termination,
    then the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/") as response:
            assert response.status == 200

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )



def test_http11_connection_with_remote_protocol_error():
    """
    If a remote protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream([b"Wait, this isn't valid HTTP!", b""])
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )



def test_http11_connection_with_incomplete_response():
    """
    We should be gracefully handling the case where the connection ends prematurely.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, wor",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request("GET", "https://example.com/")

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )



def test_http11_connection_with_local_protocol_error():
    """
    If a local protocol error occurs, then no response will be returned,
    and the connection will not be reusable.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.LocalProtocolError) as exc_info:
            conn.request("GET", "https://example.com/", headers={"Host": "\0"})

        assert str(exc_info.value) == "Illegal header value b'\\x00'"

        assert not conn.is_idle()
        assert conn.is_closed()
        assert not conn.is_available()
        assert not conn.has_expired()
        assert (
            repr(conn)
            == "<HTTP11Connection ['https://example.com:443', CLOSED, Request Count: 1]>"
        )



def test_http11_connection_handles_one_active_request():
    """
    Attempting to send a request while one is already in-flight will raise
    a ConnectionNotAvailable exception.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/"):
            with pytest.raises(httpcore.ConnectionNotAvailable):
                conn.request("GET", "https://example.com/")



def test_http11_connection_attempt_close():
    """
    A connection can only be closed when it is idle.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with conn.stream("GET", "https://example.com/") as response:
            response.read()
            assert response.status == 200
            assert response.content == b"Hello, world!"



def test_http11_request_to_incorrect_origin():
    """
    A connection can only send requests to whichever origin it is connected to.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream([])
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(RuntimeError):
            conn.request("GET", "https://other.com/")



def test_http11_expect_continue():
    """
    HTTP "100 Continue" is an interim response.
    We simply ignore it and return the final response.

    https://httpwg.org/specs/rfc9110.html#status.100
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/100
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 100 Continue\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: plain/text\r\n",
            b"Content-Length: 13\r\n",
            b"\r\n",
            b"Hello, world!",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"Hello, world!"



def test_http11_expect_continue_waits_before_reading_body():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    request_body = ExpectContinueRequestBody()
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK",
        ]
    )

    started_at = time.monotonic()
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=request_body,
            extensions={"timeout": {"read": 1.0}},
        )

    assert response.status == 200
    assert request_body.read_count == 1
    assert request_body.first_read_at is not None
    assert request_body.first_read_at >= started_at
    assert b"Hello, world!" in stream.written



@pytest.mark.parametrize("status", [401, 413])
def test_http11_expect_continue_early_rejection_does_not_read_body(status):
    origin = httpcore.Origin(b"https", b"example.com", 443)
    request_body = ExpectContinueRequestBody()
    reason = "Unauthorized" if status == 401 else "Payload Too Large"
    stream = ExpectContinueStream(
        [f"HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\n\r\n".encode()]
    )

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=request_body,
            extensions={"timeout": {"read": 1.0}},
        )

    assert response.status == status
    assert request_body.read_count == 0
    assert request_body.first_read_at is None
    assert b"Hello, world!" not in stream.written
    assert stream.closed



def test_http11_expect_continue_timeout_starts_body_once():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    request_body = ExpectContinueRequestBody()
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK",
        ],
        timeout_reads={1},
    )

    started_at = time.monotonic()
    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=request_body,
            extensions={"timeout": {"read": 0.001}},
        )

    assert response.status == 200
    assert request_body.read_count == 1
    assert request_body.first_read_at is not None
    assert request_body.first_read_at >= started_at
    assert stream.written.count(b"Hello, world!") == 1



def test_http11_expect_continue_103_does_not_reset_wait_window():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    request_body = ExpectContinueRequestBody()
    read_timeouts: list[float | None] = []
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 103 Early Hints\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK",
        ],
        read_timeouts=read_timeouts,
        slow_reads={1},
    )

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=request_body,
            extensions={"timeout": {"read": 0.03}},
        )

    assert response.status == 200
    assert read_timeouts[1] is not None
    assert read_timeouts[1] < read_timeouts[0]



def test_http11_expect_continue_send_failure_closes_body():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    request_body = OuterExpectContinueRequestBody()
    stream = RequestBodyWriteFailureStream()

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        with pytest.raises(httpcore.RemoteProtocolError):
            conn.request(
                "POST",
                "https://example.com/",
                headers={"Expect": "100-continue"},
                content=request_body,
                extensions={"timeout": {"read": 1.0}},
            )

    assert request_body.read_count == 1
    assert request_body.closed
    assert stream.closed



def test_http11_upgrade_connection():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    We should return the response, so that the network stream
    may be used for the upgraded connection.

    https://httpwg.org/specs/rfc9110.html#status.101
    https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/101
    """
    origin = httpcore.Origin(b"wss", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 101 Switching Protocols\r\n",
            b"Connection: upgrade\r\n",
            b"Upgrade: custom\r\n",
            b"\r\n",
            b"...",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]
            content = network_stream.read(max_bytes=1024)
            assert content == b"..."



def test_http11_upgrade_with_trailing_data():
    """
    HTTP "101 Switching Protocols" indicates an upgraded connection.

    In `CONNECT` and `Upgrade:` requests, we need to handover the trailing data
    in the h11.Connection object.

    https://h11.readthedocs.io/en/latest/api.html#switching-protocols
    """
    origin = httpcore.Origin(b"wss", b"example.com", 443)
    stream = httpcore.MockStream(
        # The first element of this mock network stream buffer simulates networking
        # in which response headers and data are received at once.
        # This means that "foobar" becomes trailing data.
        [
            (
                b"HTTP/1.1 101 Switching Protocols\r\n"
                b"Connection: upgrade\r\n"
                b"Upgrade: custom\r\n"
                b"\r\n"
                b"foobar"
            ),
            b"baz",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        with conn.stream(
            "GET",
            "wss://example.com/",
            headers={"Connection": "upgrade", "Upgrade": "custom"},
        ) as response:
            assert response.status == 101
            network_stream = response.extensions["network_stream"]

            content = network_stream.read(max_bytes=3)
            assert content == b"foo"
            content = network_stream.read(max_bytes=3)
            assert content == b"bar"
            content = network_stream.read(max_bytes=3)
            assert content == b"baz"

            # Lazy tests for HTTP11UpgradeStream
            network_stream.write(b"spam")
            invalid = network_stream.get_extra_info("invalid")
            assert invalid is None
            network_stream.close()



def test_http11_early_hints():
    """
    HTTP "103 Early Hints" is an interim response.
    We simply ignore it and return the final response.

    https://datatracker.ietf.org/doc/rfc8297/
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 103 Early Hints\r\n",
            b"Link: </style.css>; rel=preload; as=style\r\n",
            b"Link: </script.js.css>; rel=preload; as=style\r\n",
            b"\r\n",
            b"HTTP/1.1 200 OK\r\n",
            b"Content-Type: text/html; charset=utf-8\r\n",
            b"Content-Length: 30\r\n",
            b"Link: </style.css>; rel=preload; as=style\r\n",
            b"Link: </script.js>; rel=preload; as=script\r\n",
            b"\r\n",
            b"<html>Hello, world! ...</html>",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request(
            "GET",
            "https://example.com/",
            headers={"Expect": "continue"},
        )
        assert response.status == 200
        assert response.content == b"<html>Hello, world! ...</html>"



def test_http11_header_sub_100kb():
    """
    A connection should be able to handle a http header size up to 100kB.
    """
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = httpcore.MockStream(
        [
            b"HTTP/1.1 200 OK\r\n",  # 17
            b"Content-Type: plain/text\r\n",  # 43
            b"Cookie: " + b"x" * (100 * 1024 - 72) + b"\r\n",  # 102381
            b"Content-Length: 0\r\n",  # 102400
            b"\r\n",
            b"",
        ]
    )
    with httpcore.HTTP11Connection(
        origin=origin, stream=stream, keepalive_expiry=5.0
    ) as conn:
        response = conn.request("GET", "https://example.com/")
        assert response.status == 200
        assert response.content == b""
