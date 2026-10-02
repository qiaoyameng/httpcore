from __future__ import annotations

import typing

from ._exceptions import RemoteProtocolError


Header = typing.Tuple[bytes, bytes]


FORBIDDEN_TRAILER_FIELDS = frozenset(
    {
        b"authentication-control",
        b"authorization",
        b"cache-control",
        b"connection",
        b"content-encoding",
        b"content-length",
        b"content-security-policy",
        b"content-type",
        b"cookie",
        b"date",
        b"expect",
        b"expires",
        b"host",
        b"keep-alive",
        b"location",
        b"max-forwards",
        b"proxy-authenticate",
        b"proxy-authorization",
        b"proxy-connection",
        b"public-key-pins",
        b"range",
        b"sec-ch-ua",
        b"sec-ch-ua-arch",
        b"sec-ch-ua-bitness",
        b"sec-ch-ua-full-version",
        b"sec-ch-ua-full-version-list",
        b"sec-ch-ua-mobile",
        b"sec-ch-ua-model",
        b"sec-ch-ua-platform",
        b"sec-ch-ua-platform-version",
        b"sec-fetch-dest",
        b"sec-fetch-mode",
        b"sec-fetch-site",
        b"sec-fetch-user",
        b"set-cookie",
        b"te",
        b"trailer",
        b"transfer-encoding",
        b"upgrade",
        b"via",
        b"www-authenticate",
    }
)


def enforce_trailers(headers: typing.Sequence[Header]) -> list[Header]:
    trailers = [(name, value) for name, value in headers]
    forbidden = {
        name.lower()
        for name, value in trailers
        if name.startswith(b":") or name.lower() in FORBIDDEN_TRAILER_FIELDS
    }
    if forbidden:
        raise RemoteProtocolError(
            "Forbidden field in trailers: "
            + ", ".join(name.decode("ascii", "replace") for name in sorted(forbidden))
        )
    return trailers


class ResponseTrailers:
    def __init__(self) -> None:
        self._headers: typing.Optional[typing.List[Header]] = None

    @property
    def headers(self) -> typing.Optional[typing.List[Header]]:
        return self._headers

    def set(self, headers: typing.Sequence[Header]) -> None:
        if self._headers is not None:
            raise RemoteProtocolError("Received more than one trailers block.")
        self._headers = enforce_trailers(headers)
