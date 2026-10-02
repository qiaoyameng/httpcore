"""
Controllable network backend for the certificate pinning tests.

Real TLS handshakes are performed with `ssl.SSLObject` on top of
in-memory queues, which lets the tests:

* control exactly which of two DER certificate byte strings the server
  presents on each connection;
* inspect whether any plaintext business request bytes were written
  after the handshake, allowing us to assert that a pin mismatch blocks
  the request before it leaves the client.

The backend is deliberately transport agnostic: the client side stream
exposes synchronous `read`/`write` methods (used by the sync suite) and
coroutine `aread`/`awrite` methods (used by the async suite), both backed
by the same in-memory machinery.
"""
from __future__ import annotations

import hashlib
import ssl
import threading
import typing
from collections import deque

import h2.config
import h2.connection
import h2.events

from httpcore._backends.base import (
    AsyncNetworkBackend,
    AsyncNetworkStream,
    NetworkBackend,
    NetworkStream,
)
from tests import _certificates


class _ByteQueue:
    def __init__(self) -> None:
        self._chunks: deque[bytes] = deque()
        self._cond = threading.Condition()
        self._closed = False

    def put(self, data: bytes) -> None:
        if data:
            with self._cond:
                self._chunks.append(data)
                self._cond.notify_all()

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    def get(self, timeout: float | None = None) -> bytes:
        with self._cond:
            if not self._chunks:
                self._cond.wait(timeout=timeout)
            if not self._chunks:
                return None if self._closed else b""
            return self._chunks.popleft()

    def write_into(self, bio: ssl.MemoryBIO) -> bool:
        """
        Pull the next chunk into a MemoryBIO. Returns False on end of
        stream.
        """
        chunk = self.get(timeout=5)
        if chunk is None:
            bio.write_eof()
            return False
        if chunk:
            bio.write(chunk)
        return True


class Recording:
    def __init__(self) -> None:
        # Plaintext application request data observed by the servers.
        self.requests_received: list[bytes] = []
        # Number of TLS handshakes that completed.
        self.handshakes_completed = 0

    @property
    def request_count(self) -> int:
        return len(self.requests_received)


class _TLSServer(threading.Thread):
    """
    Performs a real server side TLS handshake using one of two configured
    certificates, then serves an HTTP/1.1 or HTTP/2 request.
    """

    def __init__(
        self,
        cert_path: str,
        key_path: str,
        http2: bool,
        client_to_server: _ByteQueue,
        server_to_client: _ByteQueue,
        recorder: Recording,
    ) -> None:
        super().__init__(daemon=True)
        self._ssl_context = _certificates.server_ssl_context(
            cert_path, key_path, http2=http2
        )
        self._http2 = http2
        self._client_to_server = client_to_server
        self._server_to_client = server_to_client
        self._recorder = recorder

    def _flush(self, outgoing: ssl.MemoryBIO) -> None:
        data = outgoing.read()
        if data:
            self._server_to_client.put(data)

    def _handshake(
        self,
        ssl_object: ssl.SSLObject,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
    ) -> None:
        while True:
            try:
                ssl_object.do_handshake()
                return
            except ssl.SSLWantReadError:
                self._flush(outgoing)
                self._client_to_server.write_into(incoming)

    def _tls_read(
        self,
        ssl_object: ssl.SSLObject,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
        max_bytes: int,
    ) -> bytes:
        while True:
            try:
                return ssl_object.read(max_bytes)
            except ssl.SSLWantReadError:
                self._flush(outgoing)
                self._client_to_server.write_into(incoming)

    def _tls_write(
        self,
        ssl_object: ssl.SSLObject,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
        data: bytes,
    ) -> None:
        while data:
            try:
                n = ssl_object.write(data)
                data = data[n:]
            except ssl.SSLWantWriteError:
                self._flush(outgoing)
                self._client_to_server.write_into(incoming)
            finally:
                self._flush(outgoing)

    def _serve_http11(
        self,
        ssl_object: ssl.SSLObject,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
    ) -> None:
        while True:
            request = b""
            # Read up until the end of the request headers. If the client
            # aborts after a failed pin check no application data ever
            # arrives, and the read returns empty once closed.
            while b"\r\n\r\n" not in request:
                chunk = self._tls_read(ssl_object, incoming, outgoing, 4096)
                if not chunk:
                    return
                request += chunk
            self._recorder.requests_received.append(request)
            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Length: 2\r\n"
                b"\r\nok"
            )
            self._tls_write(ssl_object, incoming, outgoing, response)

    def _serve_http2(
        self,
        ssl_object: ssl.SSLObject,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
    ) -> None:
        config = h2.config.H2Configuration(
            client_side=False, validate_inbound_headers=False
        )
        conn = h2.connection.H2Connection(config=config)
        conn.initiate_connection()
        self._tls_write(ssl_object, incoming, outgoing, conn.data_to_send())

        while True:
            data = self._tls_read(ssl_object, incoming, outgoing, 65535)
            if not data:
                return
            events = conn.receive_data(data)
            for event in events:
                if isinstance(event, h2.events.RequestReceived):
                    header_block = b"".join(
                        name + value for name, value in event.headers
                    )
                    self._recorder.requests_received.append(header_block)
                elif isinstance(event, h2.events.StreamEnded):
                    conn.send_headers(
                        event.stream_id,
                        [(b":status", b"200"), (b"content-length", b"2")],
                    )
                    conn.send_data(event.stream_id, b"ok", end_stream=True)
                elif isinstance(event, h2.events.ConnectionTerminated):
                    return
                elif isinstance(event, h2.events.WindowUpdated):
                    continue
            self._tls_write(
                ssl_object, incoming, outgoing, conn.data_to_send()
            )

    def run(self) -> None:
        incoming = ssl.MemoryBIO()
        outgoing = ssl.MemoryBIO()
        ssl_object = self._ssl_context.wrap_bio(
            incoming, outgoing, server_side=True
        )
        try:
            self._handshake(ssl_object, incoming, outgoing)
            self._flush(outgoing)
            self._recorder.handshakes_completed += 1
            if ssl_object.selected_alpn_protocol() == "h2":
                self._serve_http2(ssl_object, incoming, outgoing)
            else:
                self._serve_http11(ssl_object, incoming, outgoing)
        except Exception:
            # A pin failure tears down the client side before any request
            # bytes are sent, which surfaces here as an unexpected EOF.
            return


class PinningStream:
    """
    Client side of the in-memory TLS connection. Performs a real client
    handshake, records the presented DER certificate, and exposes it via
    `get_extra_info("ssl_object")`.
    """

    def __init__(
        self,
        cert_path: str,
        key_path: str,
        http2: bool,
        recorder: Recording,
    ) -> None:
        self._cert_path = cert_path
        self._key_path = key_path
        self._http2 = http2
        self._recorder = recorder

        self._client_to_server = _ByteQueue()
        self._server_to_client = _ByteQueue()

        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl_object: ssl.SSLObject | None = None
        self._closed = False
        self._read_lock = threading.Lock()
        self._write_lock = threading.Lock()

    def _start_tls(
        self, ssl_context: ssl.SSLContext, server_hostname: str | None
    ) -> None:
        server = _TLSServer(
            cert_path=self._cert_path,
            key_path=self._key_path,
            http2=self._http2,
            client_to_server=self._client_to_server,
            server_to_client=self._server_to_client,
            recorder=self._recorder,
        )
        server.start()

        self._ssl_object = ssl_context.wrap_bio(
            self._incoming,
            self._outgoing,
            server_hostname=server_hostname,
        )
        while True:
            try:
                self._ssl_object.do_handshake()
                return
            except ssl.SSLWantReadError:
                self._flush_outgoing()
                chunk = self._server_to_client.get(timeout=5)
                if chunk:
                    self._incoming.write(chunk)

    def _flush_outgoing(self) -> None:
        data = self._outgoing.read()
        if data:
            self._client_to_server.put(data)

    def _read(self, max_bytes: int) -> bytes:
        # Reads are already serialized by httpcore's per-connection read lock.
        assert self._ssl_object is not None
        while True:
            try:
                return self._ssl_object.read(max_bytes)
            except ssl.SSLWantReadError:
                with self._write_lock:
                    self._flush_outgoing()
                chunk = self._server_to_client.get(timeout=5)
                if chunk:
                    with self._write_lock:
                        self._incoming.write(chunk)

    def _write(self, data: bytes) -> None:
        # Writes are already serialized by httpcore's per-connection write lock.
        assert self._ssl_object is not None
        while data:
            try:
                n = self._ssl_object.write(data)
                data = data[n:]
            except ssl.SSLWantWriteError:
                chunk = self._server_to_client.get(timeout=5)
                if chunk:
                    self._incoming.write(chunk)
            finally:
                with self._write_lock:
                    self._flush_outgoing()

    def _close(self) -> None:
        with self._write_lock:
            if not self._closed:
                self._closed = True
                if self._ssl_object is not None:
                    try:
                        self._ssl_object.unwrap()
                    except Exception:
                        pass
                self._flush_outgoing()
                # Signal end of the record stream so a waiting server wakes.
                self._client_to_server.close()

    def _get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return self._ssl_object
        return None

    @property
    def fingerprint(self) -> bytes:
        assert self._ssl_object is not None
        der = self._ssl_object.getpeercert(binary_form=True)
        assert der is not None
        return hashlib.sha256(der).digest()


class SyncPinningStream(PinningStream, NetworkStream):
    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return self._read(max_bytes)

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._write(buffer)

    def close(self) -> None:
        self._close()

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> NetworkStream:
        self._start_tls(ssl_context, server_hostname)
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        return self._get_extra_info(info)


class AsyncPinningStream(PinningStream, AsyncNetworkStream):
    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return self._read(max_bytes)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._write(buffer)

    async def aclose(self) -> None:
        self._close()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        self._start_tls(ssl_context, server_hostname)
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        return self._get_extra_info(info)


class PinningBackend(NetworkBackend):
    """
    Each new TCP connection presents the next certificate in the configured
    rotation, defaulting to two distinct certificates.
    """

    def __init__(
        self,
        cert_rotation: typing.Sequence[tuple[str, str]] | None = None,
        http2: bool = False,
    ) -> None:
        if cert_rotation is None:
            cert_rotation = [
                (_certificates.CERT_A, _certificates.KEY_A),
                (_certificates.CERT_B, _certificates.KEY_B),
            ]
        self._cert_rotation = list(cert_rotation)
        self._next_cert = 0
        self._http2 = http2
        self.recorder = Recording()
        self.streams: list[PinningStream] = []

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Any = None,
    ) -> NetworkStream:
        cert_path, key_path = self._cert_rotation[
            self._next_cert % len(self._cert_rotation)
        ]
        self._next_cert += 1
        stream = SyncPinningStream(
            cert_path=cert_path,
            key_path=key_path,
            http2=self._http2,
            recorder=self.recorder,
        )
        self.streams.append(stream)
        return stream

    @property
    def fingerprints(self) -> list[bytes]:
        return [stream.fingerprint for stream in self.streams]

    def connect_unix_socket(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        raise NotImplementedError()

    def sleep(self, seconds: float) -> None:
        pass


class AsyncPinningBackend(AsyncNetworkBackend):
    def __init__(
        self,
        cert_rotation: typing.Sequence[tuple[str, str]] | None = None,
        http2: bool = False,
    ) -> None:
        if cert_rotation is None:
            cert_rotation = [
                (_certificates.CERT_A, _certificates.KEY_A),
                (_certificates.CERT_B, _certificates.KEY_B),
            ]
        self._cert_rotation = list(cert_rotation)
        self._next_cert = 0
        self._http2 = http2
        self.recorder = Recording()
        self.streams: list[PinningStream] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Any = None,
    ) -> AsyncNetworkStream:
        cert_path, key_path = self._cert_rotation[
            self._next_cert % len(self._cert_rotation)
        ]
        self._next_cert += 1
        stream = AsyncPinningStream(
            cert_path=cert_path,
            key_path=key_path,
            http2=self._http2,
            recorder=self.recorder,
        )
        self.streams.append(stream)
        return stream

    @property
    def fingerprints(self) -> list[bytes]:
        return [stream.fingerprint for stream in self.streams]

    async def connect_unix_socket(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        raise NotImplementedError()

    async def sleep(self, seconds: float) -> None:
        pass


def wait_for_settle(timeout: float = 0.25) -> None:
    """Give the daemon server threads time to finish their handshake."""
    import time as _time

    _time.sleep(timeout)


class SyncTunnelPinningBackend(PinningBackend):
    """
    Simulates an HTTP CONNECT proxy in front of the TLS target. The CONNECT
    exchange is plaintext; only the tunneled target presents the certificate.
    """

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Any = None,
    ) -> NetworkStream:
        cert_path, key_path = self._cert_rotation[
            self._next_cert % len(self._cert_rotation)
        ]
        self._next_cert += 1
        tls_stream = SyncPinningStream(
            cert_path=cert_path,
            key_path=key_path,
            http2=self._http2,
            recorder=self.recorder,
        )
        self.streams.append(tls_stream)
        return SyncConnectStream(tls_stream)


class SyncConnectStream(NetworkStream):
    def __init__(self, tls_stream: SyncPinningStream) -> None:
        self._tls_stream = tls_stream
        self._buffer = b"HTTP/1.1 200 Connection Established\r\n\r\n"
        self._upgraded = False

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._buffer:
            data, self._buffer = self._buffer, b""
            return data
        return self._tls_stream.read(max_bytes, timeout)

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        # Writes before the TLS upgrade are the plaintext CONNECT request.
        if not self._upgraded:
            return
        self._tls_stream.write(buffer, timeout)

    def close(self) -> None:
        self._tls_stream.close()

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> NetworkStream:
        stream = self._tls_stream.start_tls(ssl_context, server_hostname, timeout)
        self._upgraded = True
        return stream

    def get_extra_info(self, info: str) -> typing.Any:
        return None


class AsyncTunnelPinningBackend(AsyncPinningBackend):
    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Any = None,
    ) -> AsyncNetworkStream:
        cert_path, key_path = self._cert_rotation[
            self._next_cert % len(self._cert_rotation)
        ]
        self._next_cert += 1
        tls_stream = AsyncPinningStream(
            cert_path=cert_path,
            key_path=key_path,
            http2=self._http2,
            recorder=self.recorder,
        )
        self.streams.append(tls_stream)
        return AsyncConnectStream(tls_stream)


class AsyncConnectStream(AsyncNetworkStream):
    def __init__(self, tls_stream: AsyncPinningStream) -> None:
        self._tls_stream = tls_stream
        self._buffer = b"HTTP/1.1 200 Connection Established\r\n\r\n"
        self._upgraded = False

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._buffer:
            data, self._buffer = self._buffer, b""
            return data
        return await self._tls_stream.read(max_bytes, timeout)

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        if not self._upgraded:
            return
        await self._tls_stream.write(buffer, timeout)

    async def aclose(self) -> None:
        await self._tls_stream.aclose()

    async def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> AsyncNetworkStream:
        stream = await self._tls_stream.start_tls(
            ssl_context, server_hostname, timeout
        )
        self._upgraded = True
        return stream

    def get_extra_info(self, info: str) -> typing.Any:
        return None


class RawRecordingStream(NetworkStream):
    """A plain TCP stream that records any business request bytes."""

    def __init__(self, recorder: Recording) -> None:
        self._recorder = recorder
        self._closed = False

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return b""

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._recorder.requests_received.append(bytes(buffer))

    def close(self) -> None:
        self._closed = True

    def start_tls(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        raise NotImplementedError()

    def get_extra_info(self, info: str) -> typing.Any:
        return None


class RawRecordingBackend(NetworkBackend):
    def __init__(self) -> None:
        self.recorder = Recording()

    def connect_tcp(self, *args: typing.Any, **kwargs: typing.Any) -> NetworkStream:
        return RawRecordingStream(self.recorder)

    def connect_unix_socket(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        raise NotImplementedError()

    def sleep(self, seconds: float) -> None:
        pass


class AsyncRawRecordingStream(AsyncNetworkStream):
    def __init__(self, recorder: Recording) -> None:
        self._recorder = recorder

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return b""

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._recorder.requests_received.append(bytes(buffer))

    async def aclose(self) -> None:
        pass

    async def start_tls(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        raise NotImplementedError()

    def get_extra_info(self, info: str) -> typing.Any:
        return None


class AsyncRawRecordingBackend(AsyncNetworkBackend):
    def __init__(self) -> None:
        self.recorder = Recording()

    async def connect_tcp(self, *args: typing.Any, **kwargs: typing.Any) -> AsyncNetworkStream:
        return AsyncRawRecordingStream(self.recorder)

    async def connect_unix_socket(self, *args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        raise NotImplementedError()

    async def sleep(self, seconds: float) -> None:
        pass
