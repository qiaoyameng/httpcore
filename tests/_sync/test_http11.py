import time

import pytest

import httpcore



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



class ExpectContinueBody:
    def __init__(self) -> None:
        self.first_read_at: float | None = None
        self.read_count = 0
        self.closed = False

    def __iter__(self) -> "ExpectContinueBody":
        return self

    def __next__(self) -> bytes:
        if self.read_count:
            raise StopIteration
        self.first_read_at = time.monotonic()
        self.read_count += 1
        return b"Hello, world!"

    def close(self) -> None:
        self.closed = True


class ExpectContinueStream(httpcore.NetworkStream):
    def __init__(self, responses: list[bytes], *, fail_body_write: bool = False):
        self._responses = responses
        self._fail_body_write = fail_body_write
        self.header_write_count = 0
        self.body_write_count = 0
        self.closed = False
        self.continue_at: float | None = None
        self.final_response_at: float | None = None
        self.timeout_at: float | None = None

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if not self._responses:
            self.timeout_at = time.monotonic()
            raise httpcore.ReadTimeout()
        response = self._responses.pop(0)
        if response.startswith(b"HTTP/1.1 100"):
            self.continue_at = time.monotonic()
        elif response.startswith(b"HTTP/1.1 4"):
            self.final_response_at = time.monotonic()
        return response

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if self.header_write_count == 0:
            self.header_write_count = 1
        elif self._fail_body_write and buffer != b"0\r\n\r\n":
            raise httpcore.WriteError()
        elif buffer != b"0\r\n\r\n":
            self.body_write_count += 1

    def close(self) -> None:
        self.closed = True

    def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        return self

    def get_extra_info(self, info):
        return None


class TimeoutThenContinueStream(ExpectContinueStream):
    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        response = self._responses.pop(0)
        if response == b"timeout":
            self.timeout_at = time.monotonic()
            raise httpcore.ReadTimeout()
        if response.startswith(b"HTTP/1.1 100"):
            self.continue_at = time.monotonic()
        return response


class EarlyHintsThenTimedOutStream(ExpectContinueStream):
    def __init__(self):
        super().__init__([])
        self._stage = 0

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._stage == 0:
            self._stage = 1
            time.sleep(0.01)
            return b"HTTP/1.1 103 Early Hints\r\n\r\n"
        if self._stage == 1:
            self._stage = 2
            assert timeout is not None
            assert timeout < 0.04
            self.timeout_at = time.monotonic()
            raise httpcore.ReadTimeout()
        return b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"


def test_http11_expect_continue_waits_for_100():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        ]
    )
    body = ExpectContinueBody()

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=body,
            extensions={"timeout": {"read": 10.0}},
        )
        response.close()

    assert response.status == 200
    assert stream.continue_at is not None
    assert body.read_count == 1
    assert body.first_read_at is not None
    assert body.first_read_at >= stream.continue_at


def test_http11_expect_continue_early_rejection_does_not_read_body():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\n\r\n"]
    )
    body = ExpectContinueBody()

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=body,
            extensions={"timeout": {"read": 10.0}},
        )
        response.close()

    assert response.status == 413
    assert stream.final_response_at is not None
    assert stream.body_write_count == 0
    assert body.read_count == 0
    assert body.first_read_at is None
    assert not body.closed


def test_http11_expect_continue_unauthorized_does_not_read_body():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n"]
    )
    body = ExpectContinueBody()

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=body,
            extensions={"timeout": {"read": 10.0}},
        )
        response.close()

    assert response.status == 401
    assert stream.body_write_count == 0
    assert body.read_count == 0
    assert body.first_read_at is None


def test_http11_expect_continue_early_hints_does_not_reset_timeout():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = EarlyHintsThenTimedOutStream()
    body = ExpectContinueBody()

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=body,
            extensions={"timeout": {"expect_continue": 0.05, "read": 10.0}},
        )
        response.close()

    assert response.status == 200
    assert stream.timeout_at is not None
    assert stream.body_write_count == 1
    assert body.read_count == 1


def test_http11_expect_continue_timeout_then_100_starts_body_once():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = TimeoutThenContinueStream(
        [
            b"timeout",
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
        ]
    )
    body = ExpectContinueBody()

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=body,
            extensions={"timeout": {"expect_continue": 0.01}},
        )
        response.close()

    assert response.status == 200
    assert stream.timeout_at is not None
    assert body.read_count == 1
    assert body.first_read_at is not None
    assert body.first_read_at >= stream.timeout_at


def test_http11_expect_continue_write_failure_closes_started_body():
    origin = httpcore.Origin(b"https", b"example.com", 443)
    stream = ExpectContinueStream(
        [
            b"HTTP/1.1 100 Continue\r\n\r\n",
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\n\r\n",
        ],
        fail_body_write=True,
    )
    body = ExpectContinueBody()

    with httpcore.HTTP11Connection(origin=origin, stream=stream) as conn:
        response = conn.request(
            "POST",
            "https://example.com/",
            headers={"Expect": "100-continue"},
            content=body,
            extensions={"timeout": {"read": 10.0}},
        )
        response.close()

    assert response.status == 413
    assert body.read_count == 1
    assert body.closed


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
