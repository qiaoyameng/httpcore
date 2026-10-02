from __future__ import annotations

import sys
import typing


ConnectionType = typing.TypeVar("ConnectionType")
RequestType = typing.TypeVar("RequestType")
OriginType = typing.TypeVar("OriginType")


class FairConnectionScheduler(typing.Generic[ConnectionType, RequestType, OriginType]):
    def __init__(
        self,
        *,
        max_connections: int,
        max_connections_per_origin: int | None = None,
    ) -> None:
        self._max_connections = max_connections
        self._max_connections_per_origin = (
            sys.maxsize
            if max_connections_per_origin is None
            else max_connections_per_origin
        )
        self._last_origin: OriginType | None = None

    def assign(
        self,
        *,
        requests: typing.Sequence[RequestType],
        connections: typing.MutableSequence[ConnectionType],
        origin: typing.Callable[[RequestType], OriginType],
        is_queued: typing.Callable[[RequestType], bool],
        assigned_connection: typing.Callable[
            [RequestType], ConnectionType | None
        ],
        can_handle_request: typing.Callable[[ConnectionType, OriginType], bool],
        is_available: typing.Callable[[ConnectionType], bool],
        is_idle: typing.Callable[[ConnectionType], bool],
        is_multiplexed: typing.Callable[[ConnectionType], bool],
        assign_to_connection: typing.Callable[
            [RequestType, ConnectionType, bool], None
        ],
        create_connection: typing.Callable[[OriginType], ConnectionType],
    ) -> list[ConnectionType]:
        closing_connections: list[ConnectionType] = []

        while self._assign_existing_connection(
            requests=requests,
            connections=connections,
            origin=origin,
            is_queued=is_queued,
            can_handle_request=can_handle_request,
            is_available=is_available,
            is_multiplexed=is_multiplexed,
            assign_to_connection=assign_to_connection,
        ):
            pass

        while self._assign_physical_connection(
            requests=requests,
            connections=connections,
            origin=origin,
            is_queued=is_queued,
            assigned_connection=assigned_connection,
            can_handle_request=can_handle_request,
            is_available=is_available,
            is_idle=is_idle,
            assign_to_connection=assign_to_connection,
            create_connection=create_connection,
            closing_connections=closing_connections,
        ):
            if len(connections) >= self._max_connections:
                break

        return closing_connections

    def _ordered_origins(
        self,
        requests: typing.Sequence[RequestType],
        origin: typing.Callable[[RequestType], OriginType],
        is_queued: typing.Callable[[RequestType], bool],
    ) -> list[OriginType]:
        origins: list[OriginType] = []
        for pool_request in requests:
            if not is_queued(pool_request):
                continue
            request_origin = origin(pool_request)
            if request_origin not in origins:
                origins.append(request_origin)

        start = 0
        if self._last_origin in origins:
            start = origins.index(self._last_origin) + 1
            if start == len(origins):
                start = 0
        return origins[start:] + origins[:start]

    def _head_request(
        self,
        requests: typing.Sequence[RequestType],
        origin: typing.Callable[[RequestType], OriginType],
        is_queued: typing.Callable[[RequestType], bool],
        request_origin: OriginType,
    ) -> RequestType:
        return next(
            pool_request
            for pool_request in requests
            if is_queued(pool_request) and origin(pool_request) == request_origin
        )

    def _assign_existing_connection(
        self,
        *,
        requests: typing.Sequence[RequestType],
        connections: typing.Sequence[ConnectionType],
        origin: typing.Callable[[RequestType], OriginType],
        is_queued: typing.Callable[[RequestType], bool],
        can_handle_request: typing.Callable[[ConnectionType, OriginType], bool],
        is_available: typing.Callable[[ConnectionType], bool],
        is_multiplexed: typing.Callable[[ConnectionType], bool],
        assign_to_connection: typing.Callable[
            [RequestType, ConnectionType, bool], None
        ],
    ) -> bool:
        for request_origin in self._ordered_origins(requests, origin, is_queued):
            for connection in connections:
                if (
                    can_handle_request(connection, request_origin)
                    and is_available(connection)
                    and is_multiplexed(connection)
                ):
                    head_request = self._head_request(
                        requests, origin, is_queued, request_origin
                    )
                    assign_to_connection(head_request, connection, False)
                    self._last_origin = request_origin
                    return True
        return False

    def _assign_physical_connection(
        self,
        *,
        requests: typing.Sequence[RequestType],
        connections: typing.MutableSequence[ConnectionType],
        origin: typing.Callable[[RequestType], OriginType],
        is_queued: typing.Callable[[RequestType], bool],
        assigned_connection: typing.Callable[
            [RequestType], ConnectionType | None
        ],
        can_handle_request: typing.Callable[[ConnectionType, OriginType], bool],
        is_available: typing.Callable[[ConnectionType], bool],
        is_idle: typing.Callable[[ConnectionType], bool],
        assign_to_connection: typing.Callable[
            [RequestType, ConnectionType, bool], None
        ],
        create_connection: typing.Callable[[OriginType], ConnectionType],
        closing_connections: list[ConnectionType],
    ) -> bool:
        for request_origin in self._ordered_origins(requests, origin, is_queued):
            head_request = self._head_request(
                requests, origin, is_queued, request_origin
            )
            pending_connection = assigned_connection(head_request)
            if (
                pending_connection is not None
                and pending_connection in connections
                and can_handle_request(pending_connection, request_origin)
            ):
                assign_to_connection(head_request, pending_connection, False)
                self._last_origin = request_origin
                return True

            origin_connections = [
                connection
                for connection in connections
                if can_handle_request(connection, request_origin)
            ]
            reusable_connection = next(
                (
                    connection
                    for connection in origin_connections
                    if is_available(connection) and is_idle(connection)
                ),
                None,
            )
            if reusable_connection is not None:
                assign_to_connection(head_request, reusable_connection, False)
                self._last_origin = request_origin
                return True

            if len(origin_connections) >= self._max_connections_per_origin:
                continue

            idle_connection = next(
                (connection for connection in connections if is_idle(connection)),
                None,
            )
            if len(connections) >= self._max_connections and idle_connection is None:
                return False

            if idle_connection is not None:
                connections.remove(idle_connection)
                closing_connections.append(idle_connection)

            connection = create_connection(request_origin)
            connections.append(connection)
            assign_to_connection(head_request, connection, True)
            self._last_origin = request_origin
            return True
        return False
