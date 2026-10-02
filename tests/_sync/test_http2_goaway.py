"""
Synchronous counterpart of the GOAWAY retargeting scenarios.

Drives the same in-memory scripted h2 peer through the thread-based sync
network backend, verifying frame interleaving and body transmission counts.
"""

from __future__ import annotations

import typing

import pytest

import httpcore

from tests.h2_peer import Script, SyncPeerBackend

URL = "https://example.com/"


def _body_totals(backend: SyncPeerBackend) -> list[int]:
    return [rec.total_body_bytes for rec in backend.recorders]


def _goaway_counts(backend: SyncPeerBackend) -> list[int]:
    return [len(rec.goaways) for rec in backend.recorders]


def _oneshot():
    yield b"data"


def test_goaway_retargets_replayable_request_to_new_connection():
    backend = SyncPeerBackend(
        [
            Script(goaway_on_stream=3, goaway_last_stream_id=1,
                   respond_to_streams={1}),
            Script(respond_to_streams={1}),
        ]
    )
    with httpcore.ConnectionPool(
        network_backend=backend, http2=True, http1=False
    ) as pool:
        assert pool.request("GET", URL).status == 200
        second = pool.request(
            "POST",
            URL,
            headers={b"content-length": b"4"},
            content=b"data",
            extensions={"retry_on_goaway": True},
        )
        assert second.status == 200
        assert second.content == b"ok"

    assert _body_totals(backend) == [0, 4]
    assert _goaway_counts(backend) == [1, 0]


def test_goaway_without_opt_in_is_terminal_even_for_get():
    backend = SyncPeerBackend(
        [
            Script(goaway_on_stream=3, goaway_last_stream_id=1,
                   respond_to_streams={1}),
            Script(respond_to_streams={1}),
        ]
    )
    with httpcore.ConnectionPool(
        network_backend=backend, http2=True, http1=False
    ) as pool:
        pool.request("GET", URL)
        with pytest.raises(httpcore.GoAwayError) as exc_info:
            pool.request("GET", URL)

    assert exc_info.value.stream_id == 3
    assert exc_info.value.last_stream_id == 1
    assert _body_totals(backend) == [0]
    assert len(backend.recorders) == 1


def test_goaway_consumed_oneshot_body_is_not_retargeted():
    backend = SyncPeerBackend(
        [
            Script(goaway_on_stream=3, goaway_last_stream_id=1,
                   respond_to_streams={1}),
            Script(respond_to_streams={1}),
        ]
    )
    with httpcore.ConnectionPool(
        network_backend=backend, http2=True, http1=False
    ) as pool:
        pool.request("GET", URL)
        with pytest.raises(httpcore.GoAwayError):
            pool.request(
                "POST",
                URL,
                headers={b"content-length": b"4"},
                content=_oneshot(),
                extensions={"retry_on_goaway": True},
            )

    assert _body_totals(backend) == [0]
    assert len(backend.recorders) == 1


def test_goaway_in_range_stream_completes():
    backend = SyncPeerBackend(
        [
            Script(
                goaway_on_stream=3,
                goaway_last_stream_id=3,
                goaway_on_headers=False,
                respond_to_streams={1, 3},
            ),
            Script(respond_to_streams={1}),
        ]
    )
    with httpcore.ConnectionPool(
        network_backend=backend, http2=True, http1=False
    ) as pool:
        assert pool.request("GET", URL).status == 200
        second = pool.request(
            "POST",
            URL,
            headers={b"content-length": b"4"},
            content=b"data",
            extensions={"retry_on_goaway": True},
        )
        assert second.status == 200

    assert _body_totals(backend) == [4]
    assert _goaway_counts(backend) == [1]


def test_second_goaway_with_smaller_last_stream_id_retargets():
    backend = SyncPeerBackend(
        [
            Script(
                goaway_on_stream=3,
                goaway_last_stream_id=3,
                second_goaway_last_stream_id=1,
                respond_to_streams={1},
            ),
            Script(respond_to_streams={1}),
        ]
    )
    with httpcore.ConnectionPool(
        network_backend=backend, http2=True, http1=False
    ) as pool:
        pool.request("GET", URL)
        second = pool.request(
            "POST",
            URL,
            headers={b"content-length": b"4"},
            content=b"data",
            extensions={"retry_on_goaway": True},
        )
        assert second.status == 200

    assert _body_totals(backend) == [0, 4]
    assert _goaway_counts(backend) == [2, 0]


def test_repeated_goaway_has_bounded_retries():
    scripts = [
        Script(goaway_on_stream=1, goaway_last_stream_id=0,
               respond_to_streams=set())
        for _ in range(5)
    ]
    backend = SyncPeerBackend(scripts)
    with httpcore.ConnectionPool(
        network_backend=backend, http2=True, http1=False
    ) as pool:
        with pytest.raises(httpcore.GoAwayError):
            pool.request(
                "POST",
                URL,
                headers={b"content-length": b"4"},
                content=b"data",
                extensions={"retry_on_goaway": True},
            )

    assert len(backend.recorders) == (
        httpcore.ConnectionPool.MAX_GOAWAY_RETRIES + 1
    )


def test_queued_request_unblocks_on_goaway_and_retargets():
    import threading

    backend = SyncPeerBackend(
        [
            Script(
                max_concurrent_streams=1,
                goaway_on_stream=1,
                goaway_last_stream_id=0,
                respond_to_streams=set(),
            ),
            Script(respond_to_streams={1}, max_concurrent_streams=1),
        ]
    )
    result: list = []

    def worker() -> None:
        with httpcore.ConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            try:
                response = pool.request(
                    "GET", URL, extensions={"retry_on_goaway": True}
                )
                result.append(response.status)
            except Exception as exc:  # pragma: no cover - diagnostic
                result.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=5.0)
    assert not thread.is_alive()
    assert result == [200]
    assert len(backend.recorders) == 2






class _ReadTimeoutStream(httpcore.NetworkStream):
    """Completes the h2 handshake, then raises ReadTimeout on further reads."""

    def __init__(self, preface: bytes) -> None:
        self._preface = preface
        self._read = 0

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._read == 0:
            self._read += 1
            return self._preface
        raise httpcore.ReadTimeout("no response")

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        pass

    def close(self) -> None:
        pass

    def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        if info == "ssl_object":
            class _SSL:
                @staticmethod
                def selected_alpn_protocol() -> str:
                    return "h2"

            return _SSL()
        return None


class _ReadTimeoutBackend(httpcore.NetworkBackend):
    def __init__(self, preface: bytes) -> None:
        self._preface = preface

    def connect_tcp(self, host, port, timeout=None, local_address=None,
                    socket_options=None) -> httpcore.NetworkStream:
        return _ReadTimeoutStream(self._preface)


def test_waiting_read_has_bounded_exit_on_timeout():
    """
    When the server never sends a response the synchronous path surfaces a
    diagnosable ReadTimeout rather than hanging indefinitely.
    """
    import hyperframe.frame

    preface = hyperframe.frame.SettingsFrame(
        settings={3: 1}  # MAX_CONCURRENT_STREAMS
    ).serialize()
    backend = _ReadTimeoutBackend(preface)
    with httpcore.ConnectionPool(
        network_backend=backend, http2=True, http1=False
    ) as pool:
        with pytest.raises(httpcore.ReadTimeout):
            pool.request("GET", URL)


class _CountingReplayableBody(httpcore.ReplayableByteStream):
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.iterations = 0

    def __iter__(self):
        self.iterations += 1
        yield self.payload

    def __aiter__(self):  # pragma: no cover - sync tests
        self.iterations += 1
        yield self.payload


def test_replayable_stream_body_sent_once_on_new_connection():
    backend = SyncPeerBackend(
        [
            Script(goaway_on_stream=3, goaway_last_stream_id=1,
                   respond_to_streams={1}),
            Script(respond_to_streams={1}),
        ]
    )
    body = _CountingReplayableBody(b"data")
    with httpcore.ConnectionPool(
        network_backend=backend, http2=True, http1=False
    ) as pool:
        pool.request("GET", URL)
        second = pool.request(
            "POST",
            URL,
            headers={b"content-length": b"4"},
            content=body,
            extensions={"retry_on_goaway": True},
        )
        assert second.status == 200

    assert body.iterations == 2
    assert _body_totals(backend) == [0, 4]
