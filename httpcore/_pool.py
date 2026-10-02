from __future__ import annotations

import enum
import typing

T = typing.TypeVar("T")


class RequestAssignment(enum.Enum):
    AVAILABLE = "available"
    ALLOWED = "allowed"
    EVICTABLE = "evictable"
    BUSY = "busy"
    BLOCKED = "blocked"


class OriginFairQueue(typing.Generic[T]):
    """
    A FIFO queue of requests that presents one request per origin at a time.

    The head requests are visited in origin round-robin order. Origins that
    cannot currently use a connection slot are revisited before the rotation
    advances, while origins limited by their own cap do not block other
    origins.
    """

    def __init__(
        self,
        get_origin: typing.Callable[[typing.Any], typing.Any],
        is_queued: typing.Callable[[typing.Any], bool],
    ) -> None:
        self._get_origin = get_origin
        self._is_queued = is_queued
        self._last_origin: typing.Any = None

    def get(
        self,
        requests: typing.Sequence[T],
        can_assign: typing.Callable[[T], RequestAssignment],
    ) -> T | None:
        queued_requests = [
            request for request in requests if self._is_queued(request)
        ]
        if not queued_requests:
            return None

        origins: list[typing.Any] = []
        for request in queued_requests:
            origin = self._get_origin(request)
            if not any(existing_origin == origin for existing_origin in origins):
                origins.append(origin)

        start_index = 0
        if self._last_origin is not None:
            for index, origin in enumerate(origins):
                if origin == self._last_origin:
                    start_index = index + 1
                    break
        else:
            start_index = 0
        if start_index:
            origins = origins[start_index:] + origins[:start_index]

        for origin in origins:
            head_request = next(
                request
                for request in queued_requests
                if self._get_origin(request) == origin
            )
            assignment = can_assign(head_request)

            if assignment is RequestAssignment.AVAILABLE:
                self._last_origin = origin
                return head_request
            if assignment is RequestAssignment.ALLOWED:
                self._last_origin = origin
                return head_request
            if assignment is RequestAssignment.EVICTABLE:
                self._last_origin = origin
                return head_request
            if assignment is RequestAssignment.BUSY:
                return None

        return None

    def reset(self) -> None:
        self._last_origin = None
