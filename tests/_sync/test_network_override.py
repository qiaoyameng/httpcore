import ssl
import typing

import pytest

import httpcore
from httpcore import SOCKET_OPTION

RESPONSE = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello"
CONNECT_RESPONSE = b"HTTP/1.1 200 OK\r\n\r\n"


class RecordingStream(httpcore.NetworkStream):
    """
    A fake network stream that serves canned HTTP responses, and records
    anything written to it.
    """

    def __init__(
        self, backend: "RecordingBackend", responses: typing.List[bytes]
    ) -> None:
        self._backend = backend
        self._responses = responses
        self.written = b""
        self._closed = False

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        if self._closed:
            raise httpcore.ReadError("Connection closed")
        if not self._responses:
            self._responses.append(RESPONSE)
        return self._responses.pop(0)

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        self.written += buffer

    def close(self) -> None:
        self._closed = True

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> "RecordingStream":
        self._backend.tls_calls.append(
            {"ssl_context": ssl_context, "server_hostname": server_hostname}
        )
        return self

    def get_extra_info(self, info: str) -> typing.Any:
        return None


class RecordingBackend(httpcore.NetworkBackend):
    """
    A fake network backend that records the arguments used to establish
    connections and to upgrade them to TLS.
    """

    def __init__(self, first_responses: typing.List[bytes] | None = None) -> None:
        self._first_responses = list(first_responses or [])
        self.connect_calls: typing.List[tuple] = []
        self.tls_calls: typing.List[dict] = []
        self.streams: typing.List[RecordingStream] = []

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> RecordingStream:
        self.connect_calls.append(("tcp", host, port))
        return self._stream()

    def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: typing.Iterable[SOCKET_OPTION] | None = None,
    ) -> RecordingStream:
        self.connect_calls.append(("uds", path))
        return self._stream()

    def _stream(self) -> RecordingStream:
        stream = RecordingStream(self, list(self._first_responses))
        self.streams.append(stream)
        return stream

    def sleep(self, seconds: float) -> None:
        pass


def test_network_override_validation():
    with pytest.raises(ValueError):
        # At least one of 'host', 'port' or 'uds' is required.
        httpcore.NetworkOverride()
    with pytest.raises(ValueError):
        # A TCP host cannot be combined with a Unix domain socket.
        httpcore.NetworkOverride(host="10.0.0.1", uds="/tmp/gray.sock")
    with pytest.raises(ValueError):
        # A TCP port cannot be combined with a Unix domain socket.
        httpcore.NetworkOverride(port=8443, uds="/tmp/gray.sock")
    with pytest.raises(ValueError):
        httpcore.NetworkOverride(host="")
    with pytest.raises(ValueError):
        httpcore.NetworkOverride(uds="")
    with pytest.raises(ValueError):
        httpcore.NetworkOverride(host="10.0.0.1", port=0)
    with pytest.raises(ValueError):
        httpcore.NetworkOverride(host="10.0.0.1", port=65536)
    with pytest.raises(ValueError):
        httpcore.NetworkOverride(host="10.0.0.1", applies_to="elsewhere")

    override = httpcore.NetworkOverride(host="10.0.0.1", port=8443)
    assert override == httpcore.NetworkOverride(host="10.0.0.1", port=8443)
    assert override != httpcore.NetworkOverride(host="10.0.0.2", port=8443)
    assert override != httpcore.NetworkOverride(host="10.0.0.1", port=443)
    assert repr(override) == (
        "NetworkOverride(host='10.0.0.1', port=8443, uds=None, applies_to=None)"
    )



def test_pool_does_not_share_connections_across_network_overrides():
    """
    Requests to the same origin but with different network overrides must
    not be placed on the same reusable connection, while the `Host` header,
    SNI hostname, and TLS verification continue to use the request URL.
    """
    backend = RecordingBackend()
    with httpcore.ConnectionPool(network_backend=backend) as pool:
        override_a = httpcore.NetworkOverride(host="10.0.0.1", port=8443)
        override_b = httpcore.NetworkOverride(host="10.0.0.2", port=8443)

        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"network_override": override_a},
        )
        assert response.status == 200
        assert response.content == b"hello"

        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"network_override": override_b},
        )
        assert response.status == 200

        # A request matching the first override reuses the first connection.
        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={"network_override": override_a},
        )
        assert response.status == 200

        # Exactly two connections were established, one per override.
        # The third request did not open a new connection.
        assert backend.connect_calls == [
            ("tcp", "10.0.0.1", 8443),
            ("tcp", "10.0.0.2", 8443),
        ]
        assert len(pool.connections) == 2

        # TLS was established against the original hostname, not the override.
        assert [call["server_hostname"] for call in backend.tls_calls] == [
            "example.com",
            "example.com",
        ]

        # The `Host` header used the original hostname, not the override.
        assert b"host: example.com" in backend.streams[0].written.lower()
        assert b"host: example.com" in backend.streams[1].written.lower()



def test_pool_reuses_connection_for_equal_network_override():
    """
    Equal network overrides share connections, even across instances.
    """
    backend = RecordingBackend()
    with httpcore.ConnectionPool(network_backend=backend) as pool:
        pool.request(
            "GET",
            "https://example.com/",
            extensions={"network_override": httpcore.NetworkOverride("10.0.0.1")},
        )
        pool.request(
            "GET",
            "https://example.com/",
            extensions={"network_override": httpcore.NetworkOverride("10.0.0.1")},
        )

        assert backend.connect_calls == [("tcp", "10.0.0.1", 443)]
        assert len(pool.connections) == 1



def test_pool_does_not_share_connection_between_override_and_plain():
    """
    A request without an override must not reuse an overridden connection,
    and vice versa.
    """
    backend = RecordingBackend()
    with httpcore.ConnectionPool(network_backend=backend) as pool:
        pool.request("GET", "https://example.com/")
        pool.request(
            "GET",
            "https://example.com/",
            extensions={"network_override": httpcore.NetworkOverride("10.0.0.1")},
        )

        assert backend.connect_calls == [
            ("tcp", "example.com", 443),
            ("tcp", "10.0.0.1", 443),
        ]
        assert len(pool.connections) == 2



def test_network_override_ipv6():
    """
    An IPv6 literal is a valid TCP override target.
    """
    backend = RecordingBackend()
    with httpcore.ConnectionPool(network_backend=backend) as pool:
        response = pool.request(
            "GET",
            "http://example.com/",
            extensions={
                "network_override": httpcore.NetworkOverride("::1", port=8080)
            },
        )
        assert response.status == 200

        assert backend.connect_calls == [("tcp", "::1", 8080)]
        assert b"host: example.com" in backend.streams[0].written.lower()



def test_network_override_unix_socket():
    """
    A Unix domain socket override connects over `uds` instead of TCP.
    """
    backend = RecordingBackend()
    with httpcore.ConnectionPool(network_backend=backend) as pool:
        response = pool.request(
            "GET",
            "http://example.com/",
            extensions={
                "network_override": httpcore.NetworkOverride(uds="/tmp/gray.sock")
            },
        )
        assert response.status == 200

        assert backend.connect_calls == [("uds", "/tmp/gray.sock")]
        assert b"host: example.com" in backend.streams[0].written.lower()



def test_network_override_extension_type_checked():
    backend = RecordingBackend()
    with httpcore.ConnectionPool(network_backend=backend) as pool:
        with pytest.raises(TypeError):
            pool.request(
                "GET",
                "https://example.com/",
                extensions={"network_override": "10.0.0.1"},
            )



def test_network_override_applies_to_proxy_requires_a_proxy():
    backend = RecordingBackend()
    with httpcore.ConnectionPool(network_backend=backend) as pool:
        with pytest.raises(ValueError):
            pool.request(
                "GET",
                "https://example.com/",
                extensions={
                    "network_override": httpcore.NetworkOverride(
                        "10.0.0.1", applies_to="proxy"
                    )
                },
            )



def test_network_override_via_proxy_requires_applies_to():
    """
    Via a proxy, an override must state if it targets the proxy address
    or the tunnel target. Ambiguous overrides are rejected.
    """
    backend = RecordingBackend()
    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"), network_backend=backend
    ) as pool:
        with pytest.raises(ValueError):
            pool.request(
                "GET",
                "https://example.com/",
                extensions={
                    "network_override": httpcore.NetworkOverride("10.0.0.1")
                },
            )

        assert backend.connect_calls == []



def test_proxy_tunnel_network_override_targeting_origin():
    """
    A tunnel override with 'applies_to="origin"' retargets the CONNECT
    request, while the proxy address and TLS verification are unchanged.
    """
    backend = RecordingBackend(first_responses=[CONNECT_RESPONSE])
    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"), network_backend=backend
    ) as pool:
        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={
                "network_override": httpcore.NetworkOverride(
                    "10.0.0.9", port=4443, applies_to="origin"
                )
            },
        )
        assert response.status == 200

        # The TCP connection is made to the proxy, as usual.
        assert backend.connect_calls == [("tcp", "localhost", 8080)]
        # The CONNECT request targets the override address.
        assert b"CONNECT 10.0.0.9:4443 HTTP/1.1" in backend.streams[0].written
        # TLS verification still uses the request URL hostname.
        assert [call["server_hostname"] for call in backend.tls_calls] == [
            "example.com"
        ]



def test_proxy_tunnel_network_override_targeting_proxy():
    """
    A tunnel override with 'applies_to="proxy"' changes the address used
    to reach the proxy, while the tunnel target is unchanged.
    """
    backend = RecordingBackend(first_responses=[CONNECT_RESPONSE])
    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"), network_backend=backend
    ) as pool:
        response = pool.request(
            "GET",
            "https://example.com/",
            extensions={
                "network_override": httpcore.NetworkOverride(
                    "10.0.0.7", port=8081, applies_to="proxy"
                )
            },
        )
        assert response.status == 200

        # The TCP connection is made to the override address.
        assert backend.connect_calls == [("tcp", "10.0.0.7", 8081)]
        # The CONNECT request targets the original origin.
        assert b"CONNECT example.com:443 HTTP/1.1" in backend.streams[0].written
        assert [call["server_hostname"] for call in backend.tls_calls] == [
            "example.com"
        ]



def test_proxy_tunnel_connections_not_shared_across_overrides():
    """
    Tunneled connections to the same origin via different override
    addresses are not reused across destinations.
    """
    backend = RecordingBackend(first_responses=[CONNECT_RESPONSE])
    with httpcore.ConnectionPool(
        proxy=httpcore.Proxy("http://localhost:8080/"), network_backend=backend
    ) as pool:
        pool.request(
            "GET",
            "https://example.com/",
            extensions={
                "network_override": httpcore.NetworkOverride(
                    "10.0.0.1", applies_to="proxy"
                )
            },
        )
        pool.request(
            "GET",
            "https://example.com/",
            extensions={
                "network_override": httpcore.NetworkOverride(
                    "10.0.0.2", applies_to="proxy"
                )
            },
        )

        assert backend.connect_calls == [
            ("tcp", "10.0.0.1", 8080),
            ("tcp", "10.0.0.2", 8080),
        ]
        assert len(pool.connections) == 2
