import contextlib
import typing

ExceptionMapping = typing.Mapping[typing.Type[Exception], typing.Type[Exception]]


@contextlib.contextmanager
def map_exceptions(map: ExceptionMapping) -> typing.Iterator[None]:
    try:
        yield
    except Exception as exc:  # noqa: PIE786
        for from_exc, to_exc in map.items():
            if isinstance(exc, from_exc):
                raise to_exc(exc) from exc
        raise  # pragma: nocover


class ConnectionNotAvailable(Exception):
    pass


class GoAwayRetarget(ConnectionNotAvailable):
    """
    Raised internally when an HTTP/2 GOAWAY frame proves the server never
    processed a request (its stream id was greater than the server's
    `last_stream_id`), and the caller has explicitly allowed that request
    to be replayed.

    Subclassing `ConnectionNotAvailable` means the connection pool retries
    the request on a fresh connection. Unlike a plain
    `ConnectionNotAvailable`, however, repeated GOAWAY retargets are bounded
    by the pool, to avoid an endless loop if every new connection is
    immediately shut down.
    """


class ProxyError(Exception):
    pass


class UnsupportedProtocol(Exception):
    pass


class ProtocolError(Exception):
    pass


class RemoteProtocolError(ProtocolError):
    pass


class LocalProtocolError(ProtocolError):
    pass


class GoAwayError(RemoteProtocolError):
    """
    Raised when an HTTP/2 GOAWAY frame forces the client to abandon a stream
    that cannot safely be retargeted onto another connection.

    This is the case when the stream's identifier was greater than the
    server's `last_stream_id` (so the server definitely did not begin
    processing the request), but the request body was not replayable, or the
    caller did not explicitly opt into replaying the request.
    """

    def __init__(
        self,
        message: str = "",
        *,
        last_stream_id: int | None = None,
        stream_id: int | None = None,
        error_code: int | None = None,
    ) -> None:
        self.last_stream_id = last_stream_id
        self.stream_id = stream_id
        self.error_code = error_code
        super().__init__(message)


# Timeout errors


class TimeoutException(Exception):
    pass


class PoolTimeout(TimeoutException):
    pass


class ConnectTimeout(TimeoutException):
    pass


class ReadTimeout(TimeoutException):
    pass


class WriteTimeout(TimeoutException):
    pass


# Network errors


class NetworkError(Exception):
    pass


class ConnectError(NetworkError):
    pass


class ReadError(NetworkError):
    pass


class WriteError(NetworkError):
    pass
