"""
An in-memory, scripted HTTP/2 peer used to drive the client connection pool
through GOAWAY scenarios without any real sockets.

The peer runs an h2 server-side state machine over a pair of in-memory byte
streams. It records exactly how many times each request body was transmitted
(by inspecting completed DATA frames), and can emit one or two GOAWAY frames
interleaved with ordinary response frames.

The same script definitions are shared by the async and sync test suites.
"""

from __future__ import annotations

import struct
import threading
import typing

import h2.config
import h2.connection
import h2.events
import hyperframe.frame

import httpcore


class Script:
    """
    Behaviour of one accepted HTTP/2 connection.

    A GOAWAY is emitted as soon as the request headers for
    `goaway_on_stream` (or a higher stream id) are observed. Streams with
    ids <= the effective last-stream-id that have completed are still
    answered normally; higher streams receive no response, simulating a
    server that never processed them.
    """

    def __init__(
        self,
        *,
        max_concurrent_streams: int = 100,
        goaway_on_stream: int | None = None,
        goaway_on_headers: bool = True,
        goaway_last_stream_id: int = 0,
        goaway_error_code: int = 0,
        second_goaway_last_stream_id: int | None = None,
        respond_to_streams: typing.Iterable[int] | None = (1,),
        response_body: bytes = b"ok",
    ) -> None:
        self.max_concurrent_streams = max_concurrent_streams
        self.goaway_on_stream = goaway_on_stream
        self.goaway_on_headers = goaway_on_headers
        self.goaway_last_stream_id = goaway_last_stream_id
        self.goaway_error_code = goaway_error_code
        self.second_goaway_last_stream_id = second_goaway_last_stream_id
        self.respond_to_streams = (
            None if respond_to_streams is None else set(respond_to_streams)
        )
        self.response_body = response_body


class H2PeerRecorder:
    """Inspection state recorded by one scripted peer."""

    def __init__(self) -> None:
        self.bodies: dict[int, bytes] = {}
        self.completed_streams: list[int] = []
        self.goaways: list[int] = []

    @property
    def completed_requests(self) -> int:
        return len(self.completed_streams)

    @property
    def total_body_bytes(self) -> int:
        return sum(len(body) for body in self.bodies.values())


def _goaway_frame(last_stream_id: int, error_code: int) -> bytes:
    payload = struct.pack(
        ">II", last_stream_id & 0x7FFFFFFF, error_code
    )
    return hyperframe.frame.GoAwayFrame(
        stream_id=0,
        last_stream_id=last_stream_id,
        error_code=error_code,
    ).serialize()


def iter_h2_frames(data: bytes) -> typing.Iterator[bytes]:
    """Yield complete HTTP/2 frame byte sequences from a buffer."""
    view = memoryview(data)
    offset = 0
    while offset + 9 <= len(view):
        length = int.from_bytes(bytes(view[offset : offset + 3]), "big")
        end = offset + 9 + length
        if end > len(view):
            # Partial frame; the client will send more.
            break
        yield bytes(view[offset:end])
        offset = end


class BaseH2Peer:
    """h2 server-side logic; subclasses only add the byte pump."""

    def __init__(self, script: Script, recorder: H2PeerRecorder) -> None:
        self.script = script
        self.recorder = recorder
        self._h2 = h2.connection.H2Connection(
            config=h2.config.H2Configuration(
                client_side=False, validate_inbound_headers=False
            )
        )
        self._h2.initiate_connection()
        self._h2.update_settings(
            {
                h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: (
                    script.max_concurrent_streams
                ),
                h2.settings.SettingCodes.ENABLE_PUSH: 0,
            }
        )
        self._goaway_sent = False
        self._responded: set[int] = set()

    @property
    def effective_last_stream_id(self) -> int:
        if self.script.second_goaway_last_stream_id is not None:
            return min(
                self.script.goaway_last_stream_id,
                self.script.second_goaway_last_stream_id,
            )
        return self.script.goaway_last_stream_id

    def preface(self) -> bytes:
        return self._h2.data_to_send()

    def feed(self, data: bytes) -> bytes:
        if not data:
            return b""
        if self._goaway_sent:
            # Once the server has sent GOAWAY it must tolerate (and ignore)
            # frames for streams beyond its last-stream-id, including DATA
            # that was already in flight. The h2 state machine refuses DATA
            # outright once CLOSED, so parse frames manually and swallow
            # anything we're not going to respond to.
            return self._feed_after_goaway(data)
        events = self._h2.receive_data(data)
        for event in events:
            self._handle_event(event)
        return self._h2.data_to_send()

    def _feed_after_goaway(self, data: bytes) -> bytes:
        for raw_frame in iter_h2_frames(data):
            frame_type = raw_frame[3]
            stream_id = int.from_bytes(raw_frame[5:9], "big") & 0x7FFFFFFF
            if frame_type == 0x1 and stream_id > self.effective_last_stream_id:
                # HEADERS for a stream beyond the range: do not let h2 see
                # a new stream being opened.
                continue
            if frame_type == 0x0 and stream_id > self.effective_last_stream_id:
                # DATA for a rejected stream: ignore entirely.
                continue
            # Everything else (in-range data, window updates, pings, ...)
            # is safe to feed through the state machine.
            try:
                events = self._h2.receive_data(raw_frame)
            except h2.exceptions.ProtocolError:
                continue
            for event in events:
                self._handle_event(event)
        return self._h2.data_to_send()

    def _handle_event(self, event: h2.events.Event) -> None:
        if isinstance(event, h2.events.RequestReceived):
            if self.script.goaway_on_headers:
                # Simulate the rolling-restart race where the server has
                # already decided to shut down by the time the request
                # headers arrive. The GOAWAY is emitted before the client
                # can know the stream was rejected; the server-side h2
                # state machine is closed, so DATA for this stream then
                # arrives in `_feed_after_goaway` and is ignored.
                self._maybe_send_goaway(event.stream_id)
        elif isinstance(event, h2.events.DataReceived):
            body = self.recorder.bodies.setdefault(event.stream_id, b"")
            self.recorder.bodies[event.stream_id] = body + (event.data or b"")
            self._h2.acknowledge_received_data(
                event.flow_controlled_length or 0, event.stream_id
            )
        elif isinstance(event, h2.events.StreamEnded):
            self.recorder.completed_streams.append(event.stream_id)
            if self._should_respond(event.stream_id):
                # In-range streams get their response before the GOAWAY is
                # emitted, mirroring a server that drains in-flight work.
                self._send_response(event.stream_id)
            if not self._goaway_sent and not self.script.goaway_on_headers:
                self._maybe_send_goaway(event.stream_id)

    def _should_respond(self, stream_id: int) -> bool:
        if (
            self.script.respond_to_streams is not None
            and stream_id not in self.script.respond_to_streams
        ):
            return False
        if self._goaway_sent and stream_id > self.effective_last_stream_id:
            return False
        return True

    def _maybe_send_goaway(self, stream_id: int) -> None:
        if (
            self._goaway_sent
            or self.script.goaway_on_stream is None
            or stream_id < self.script.goaway_on_stream
        ):
            return
        self._goaway_sent = True
        self.recorder.goaways.append(self.script.goaway_last_stream_id)
        self._h2.close_connection(
            error_code=self.script.goaway_error_code,
            last_stream_id=self.script.goaway_last_stream_id,
        )
        if self.script.second_goaway_last_stream_id is not None:
            # Emit the revised (smaller) last-stream-id in the very same
            # response batch. h2 refuses a second GOAWAY after closing, so
            # build the frame raw and append it immediately.
            self.recorder.goaways.append(
                self.script.second_goaway_last_stream_id
            )
            self._extra_frame = _goaway_frame(
                self.script.second_goaway_last_stream_id,
                self.script.goaway_error_code,
            )
        # An in-flight stream whose headers arrived earlier and which is
        # within range still gets answered once its body completes; nothing
        # to do proactively here.

    def _send_response(self, stream_id: int) -> None:
        if stream_id in self._responded:
            return
        self._responded.add(stream_id)
        self._h2.send_headers(
            stream_id,
            [(b":status", b"200"), (b"content-type", b"text/plain")],
        )
        self._h2.send_data(
            stream_id, self.script.response_body, end_stream=True
        )

    def drain_extra(self) -> bytes:
        extra = getattr(self, "_extra_frame", b"")
        if extra:
            self._extra_frame = b""
        return extra


class _FakeSSL:
    def __init__(self, protocol: str) -> None:
        self._protocol = protocol

    def selected_alpn_protocol(self) -> str:
        return self._protocol


# --------------------------------------------------------------------------
# Async transport
# --------------------------------------------------------------------------


class AsyncPeerStream(httpcore.AsyncNetworkStream):
    def __init__(self, server_rx, client_rx) -> None:
        self._server_rx = server_rx
        self._client_rx = client_rx

    async def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        import anyio

        with anyio.fail_after(timeout):
            return await self._client_rx.receive()

    async def write(self, buffer: bytes, timeout: float | None = None) -> None:
        await self._server_rx.send(buffer)

    async def aclose(self) -> None:
        await self._server_rx.aclose()
        await self._client_rx.aclose()

    async def start_tls(
        self, ssl_context, server_hostname=None, timeout=None
    ) -> "AsyncPeerStream":
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return _FakeSSL("h2")
        return None


async def _async_pump(peer: BaseH2Peer, client_tx, server_rx) -> None:
    import anyio

    async with client_tx, server_rx:
        await client_tx.send(peer.preface())
        while True:
            try:
                data = await server_rx.receive()
            except anyio.EndOfStream:
                return
            out = peer.feed(data)
            extra = peer.drain_extra()
            # Send all frames produced from one client read as a single
            # item, so e.g. two GOAWAY frames in one batch reach the client
            # atomically.
            response = out + extra
            if response:
                await client_tx.send(response)


class AsyncPeerBackend(httpcore.AsyncNetworkBackend):
    """Creates a fresh in-memory h2 peer per connection."""

    def __init__(self, scripts: typing.Sequence[Script]) -> None:
        self._scripts = list(scripts)
        self._next = 0
        self.recorders: list[H2PeerRecorder] = []

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[object] | None = None,
    ) -> AsyncPeerStream:
        import anyio

        script = (
            self._scripts[self._next]
            if self._next < len(self._scripts)
            else self._scripts[-1]
        )
        self._next += 1
        recorder = H2PeerRecorder()
        self.recorders.append(recorder)

        server_tx, server_rx = anyio.create_memory_object_stream[bytes](256)
        client_tx, client_rx = anyio.create_memory_object_stream[bytes](256)
        peer = BaseH2Peer(script, recorder)
        task_group = anyio_low_level_task_group()
        task_group.start_soon(_async_pump, peer, client_tx, server_rx)
        return AsyncPeerStream(server_tx, client_rx)

    async def connect_unix_socket(self, *args: typing.Any, **kwargs: typing.Any):
        return await self.connect_tcp("", 0)

    async def sleep(self, seconds: float) -> None:
        import anyio

        await anyio.sleep(seconds)


def anyio_low_level_task_group():
    """
    Obtain the current task group so peer pumps live as long as the test
    coroutine does. Tests wrap their work in `open_peer_task_group()`.
    """
    import anyio

    return _CURRENT_TASK_GROUPS[-1]


_CURRENT_TASK_GROUPS: list[typing.Any] = []


class open_peer_task_group:
    async def __aenter__(self) -> typing.Any:
        import anyio

        self._cm = anyio.create_task_group()
        tg = await self._cm.__aenter__()
        _CURRENT_TASK_GROUPS.append(tg)
        return tg

    async def __aexit__(self, *exc: typing.Any) -> None:
        _CURRENT_TASK_GROUPS.pop()
        await self._cm.__aexit__(*exc)


# --------------------------------------------------------------------------
# Sync transport (threaded)
# --------------------------------------------------------------------------


class _SyncChannel:
    """A tiny bounded, blocking byte channel."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._items: list[bytes] = []
        self._closed = False

    def send(self, item: bytes) -> None:
        with self._cond:
            if self._closed:
                return
            self._items.append(item)
            self._cond.notify_all()

    def receive(self, timeout: float | None = None) -> bytes:
        with self._cond:
            if not self._items:
                self._cond.wait(timeout=timeout)
            if not self._items:
                # Empty payload on timeout, EOF sentinel on close.
                return b"" if self._closed else b"__idle__"
            return self._items.pop(0)

    def receive_available(self, timeout: float | None = None) -> bytes:
        """Block for the first item, then drain anything else queued."""
        with self._cond:
            if not self._items:
                self._cond.wait(timeout=timeout)
            if not self._items:
                return b"" if self._closed else b"__idle__"
            data = b"".join(self._items)
            self._items.clear()
            return data

    def drain(self) -> list[bytes]:
        with self._cond:
            items = list(self._items)
            self._items.clear()
            return items

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()


class SyncPeerStream(httpcore.NetworkStream):
    def __init__(
        self, server_tx: _SyncChannel, client_rx: _SyncChannel
    ) -> None:
        self._server_tx = server_tx
        self._client_rx = client_rx

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        data = self._client_rx.receive_available(timeout=timeout)
        if data == b"__idle__":
            raise httpcore.ReadTimeout("timed out waiting for peer frame")
        return data

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self._server_tx.send(buffer)

    def close(self) -> None:
        self._server_tx.close()

    def start_tls(
        self, ssl_context, server_hostname=None, timeout=None
    ) -> "SyncPeerStream":
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            return _FakeSSL("h2")
        return None


def _sync_pump(
    peer: BaseH2Peer,
    client_tx: _SyncChannel,
    server_rx: _SyncChannel,
) -> None:
    client_tx.send(peer.preface())
    while True:
        data = server_rx.receive(timeout=2.0)
        if data == b"":
            # Drain anything the client sent just before closing.
            remaining = server_rx.drain()
            for chunk in remaining:
                out = peer.feed(chunk)
                extra = peer.drain_extra()
                response = out + extra
                if response:
                    client_tx.send(response)
            client_tx.close()
            return
        if data == b"__idle__":
            # No new client data; keep the connection alive (the client may
            # still be waiting on a response it will never receive).
            continue
        out = peer.feed(data)
        extra = peer.drain_extra()
        response = out + extra
        if response:
            client_tx.send(response)


class SyncPeerBackend(httpcore.NetworkBackend):
    def __init__(self, scripts: typing.Sequence[Script]) -> None:
        self._scripts = list(scripts)
        self._next = 0
        self.recorders: list[H2PeerRecorder] = []
        self._threads: list[threading.Thread] = []

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[object] | None = None,
    ) -> SyncPeerStream:
        script = (
            self._scripts[self._next]
            if self._next < len(self._scripts)
            else self._scripts[-1]
        )
        self._next += 1
        recorder = H2PeerRecorder()
        self.recorders.append(recorder)

        server_tx = _SyncChannel()
        client_tx = _SyncChannel()
        peer = BaseH2Peer(script, recorder)
        thread = threading.Thread(
            target=_sync_pump,
            args=(peer, client_tx, server_tx),
            daemon=True,
        )
        thread.start()
        self._threads.append(thread)
        return SyncPeerStream(server_tx, client_tx)

    def connect_unix_socket(self, *args: typing.Any, **kwargs: typing.Any):
        return self.connect_tcp("", 0)

    def sleep(self, seconds: float) -> None:
        pass
