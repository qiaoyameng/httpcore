"""
GOAWAY retargeting tests driven by an in-memory, scripted HTTP/2 peer.

These tests control the exact interleaving of GOAWAY and response frames and
verify how many times each request body is transmitted, distinguishing:

* requests never processed by the server (stream id > last_stream_id) that
  are transparently retargeted to a new connection (only when the caller
  explicitly opts in and the body is replayable);
* streams already within the GOAWAY range that continue to completion;
* bounded termination for a shrinking last_stream_id, repeatedly rejecting
  servers, and waiting requests.
"""

from __future__ import annotations

import anyio
import pytest

import httpcore

from tests.h2_peer import (
    AsyncPeerBackend,
    Script,
    open_peer_task_group,
)

pytestmark = pytest.mark.anyio


URL = "https://example.com/"


def _body_totals(backend: AsyncPeerBackend) -> list[int]:
    return [rec.total_body_bytes for rec in backend.recorders]


def _goaway_counts(backend: AsyncPeerBackend) -> list[int]:
    return [len(rec.goaways) for rec in backend.recorders]


async def _oneshot():
    yield b"data"


async def test_goaway_retargets_replayable_request_to_new_connection():
    """
    Stream 1 completes. Stream 3 is rejected by GOAWAY(last_stream_id=1).
    With explicit permission and a byte body, it is retargeted to a fresh
    connection; its body is sent exactly once, on the new connection.
    """
    backend = AsyncPeerBackend(
        [
            Script(goaway_on_stream=3, goaway_last_stream_id=1,
                   respond_to_streams={1}),
            Script(respond_to_streams={1}),
        ]
    )
    async with open_peer_task_group():
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            first = await pool.request("GET", URL)
            assert first.status == 200

            second = await pool.request(
                "POST",
                URL,
                headers={b"content-length": b"4"},
                content=b"data",
                extensions={"retry_on_goaway": True},
            )
            assert second.status == 200
            assert second.content == b"ok"

    # Stream 3's body never reached the first peer; the retargeted request
    # (stream 1 on the new connection) delivered it exactly once.
    assert _body_totals(backend) == [0, 4]
    assert _goaway_counts(backend) == [1, 0]


async def test_goaway_without_opt_in_is_terminal_even_for_get():
    """
    Replay is never inferred from the method. A GET without
    'retry_on_goaway' rejected by GOAWAY raises GoAwayError and does not
    open a second connection.
    """
    backend = AsyncPeerBackend(
        [
            Script(goaway_on_stream=3, goaway_last_stream_id=1,
                   respond_to_streams={1}),
            Script(respond_to_streams={1}),
        ]
    )
    async with open_peer_task_group():
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            await pool.request("GET", URL)
            with pytest.raises(httpcore.GoAwayError) as exc_info:
                await pool.request("GET", URL)

    assert exc_info.value.stream_id == 3
    assert exc_info.value.last_stream_id == 1
    assert _body_totals(backend) == [0]
    assert len(backend.recorders) == 1


async def test_goaway_consumed_oneshot_body_is_not_retargeted():
    """
    A one-shot async iterator body that has already been sent cannot be
    regenerated, so the request terminates with GoAwayError even though the
    caller opted in. No second connection is used.
    """
    backend = AsyncPeerBackend(
        [
            Script(goaway_on_stream=3, goaway_last_stream_id=1,
                   respond_to_streams={1}),
            Script(respond_to_streams={1}),
        ]
    )
    async with open_peer_task_group():
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            await pool.request("GET", URL)
            with pytest.raises(httpcore.GoAwayError):
                await pool.request(
                    "POST",
                    URL,
                    headers={b"content-length": b"4"},
                    content=_oneshot(),
                    extensions={"retry_on_goaway": True},
                )

    assert _body_totals(backend) == [0]
    assert len(backend.recorders) == 1


async def test_goaway_in_range_stream_completes():
    """
    GOAWAY(last_stream_id=3) while stream 3 is in flight: the server may
    have processed it, and it does respond, so the stream completes normally
    on the original connection without retargeting.
    """
    backend = AsyncPeerBackend(
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
    async with open_peer_task_group():
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            first = await pool.request("GET", URL)
            assert first.status == 200
            second = await pool.request(
                "POST",
                URL,
                headers={b"content-length": b"4"},
                content=b"data",
                extensions={"retry_on_goaway": True},
            )
            assert second.status == 200

    # Stream 3's body was delivered once; no new connection required.
    assert _body_totals(backend) == [4]
    assert _goaway_counts(backend) == [1]


async def test_second_goaway_with_smaller_last_stream_id_retargets():
    """
    A second GOAWAY may shrink last_stream_id. After last=3 then last=1,
    stream 3 is treated as definitely unprocessed and retargeted.
    """
    backend = AsyncPeerBackend(
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
    async with open_peer_task_group():
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            await pool.request("GET", URL)
            second = await pool.request(
                "POST",
                URL,
                headers={b"content-length": b"4"},
                content=b"data",
                extensions={"retry_on_goaway": True},
            )
            assert second.status == 200

    assert _body_totals(backend) == [0, 4]
    assert _goaway_counts(backend) == [2, 0]


async def test_repeated_goaway_has_bounded_retries():
    """
    If every fresh connection immediately rejects with GOAWAY, retargeting
    is bounded and ends with a diagnosable GoAwayError rather than looping
    forever.
    """
    scripts = [
        Script(goaway_on_stream=1, goaway_last_stream_id=0,
               respond_to_streams=set())
        for _ in range(5)
    ]
    backend = AsyncPeerBackend(scripts)
    async with open_peer_task_group():
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            with pytest.raises(httpcore.GoAwayError):
                await pool.request(
                    "POST",
                    URL,
                    headers={b"content-length": b"4"},
                    content=b"data",
                    extensions={"retry_on_goaway": True},
                )

    # Initial connection plus the bounded number of retarget attempts.
    assert len(backend.recorders) == (
        httpcore.AsyncConnectionPool.MAX_GOAWAY_RETRIES + 1
    )


async def test_queued_request_unblocks_on_goaway_and_retargets():
    """
    With MAX_CONCURRENT_STREAMS=1 a second request waits for a stream slot.
    When the server GOAWAYs, the waiting request has a bounded path out of
    the wait and is retargeted (it was never sent) when it opts in.
    """
    backend = AsyncPeerBackend(
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

    async def make_requests(results: list) -> None:
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            # The first request is never answered (rejected by GOAWAY), and
            # is itself retargeted; drive a single opted-in request.
            try:
                response = await pool.request(
                    "GET", URL, extensions={"retry_on_goaway": True}
                )
                results.append(response.status)
            except Exception as exc:  # pragma: no cover - diagnostic
                results.append(exc)

    results: list = []
    async with open_peer_task_group():
        async with anyio.create_task_group() as tg:
            tg.start_soon(make_requests, results)

    assert results == [200]
    assert len(backend.recorders) == 2


async def test_waiting_request_cancellation_has_bounded_exit():
    """
    A request blocked waiting for an outgoing stream slot must respond to
    cancellation promptly, instead of being parked forever on the
    MAX_CONCURRENT_STREAMS semaphore.
    """
    backend = AsyncPeerBackend(
        [
            Script(
                max_concurrent_streams=1,
                respond_to_streams=set(),
                goaway_on_stream=None,
            )
        ]
    )

    async with open_peer_task_group():
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:

            async def hold_slot() -> None:
                try:
                    await pool.request("GET", URL)
                except BaseException:
                    pass

            async with anyio.create_task_group() as tg:
                tg.start_soon(hold_slot)
                await anyio.sleep(0.05)

                completed = False
                with anyio.move_on_after(0.2):
                    await pool.request(
                        "GET",
                        URL,
                        extensions={"retry_on_goaway": True},
                    )
                    completed = True

                assert not completed, "waiting request unexpectedly completed"
                tg.cancel_scope.cancel()


class _CountingReplayableBody(httpcore.ReplayableByteStream):
    """A regenerable request body that counts how many times it was sent."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.iterations = 0

    def __iter__(self):  # pragma: no cover - async tests
        self.iterations += 1
        yield self.payload

    async def __aiter__(self):
        self.iterations += 1
        yield self.payload


async def test_replayable_stream_body_sent_once_on_new_connection():
    backend = AsyncPeerBackend(
        [
            Script(goaway_on_stream=3, goaway_last_stream_id=1,
                   respond_to_streams={1}),
            Script(respond_to_streams={1}),
        ]
    )
    body = _CountingReplayableBody(b"data")
    async with open_peer_task_group():
        async with httpcore.AsyncConnectionPool(
            network_backend=backend, http2=True, http1=False
        ) as pool:
            await pool.request("GET", URL)
            second = await pool.request(
                "POST",
                URL,
                headers={b"content-length": b"4"},
                content=body,
                extensions={"retry_on_goaway": True},
            )
            assert second.status == 200

    # The body generator was iterated twice (once per attempt)...
    assert body.iterations == 2
    # ...but on the wire only the retargeted attempt delivered it.
    assert _body_totals(backend) == [0, 4]
