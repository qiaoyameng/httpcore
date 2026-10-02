import pytest

import httpcore
from httpcore._pins import normalize_pins
from tests._certificates import (
    CERT_A,
    CERT_B,
    FINGERPRINT_A,
    FINGERPRINT_B,
    client_ssl_context,
)
from tests._pinning_backend import (
    AsyncPinningBackend,
    AsyncRawRecordingBackend,
    AsyncTunnelPinningBackend,
    wait_for_settle,
)

pytestmark = pytest.mark.anyio


async def test_normalize_pins():
    assert normalize_pins([FINGERPRINT_A]) == frozenset({FINGERPRINT_A})
    assert normalize_pins([FINGERPRINT_A.hex()]) == frozenset({FINGERPRINT_A})
    assert normalize_pins([FINGERPRINT_A.hex().encode("ascii")]) == frozenset(
        {FINGERPRINT_A}
    )
    # Duplicate pins collapse.
    assert normalize_pins([FINGERPRINT_A, FINGERPRINT_A.hex()]) == frozenset(
        {FINGERPRINT_A}
    )


async def test_normalize_pins_rejects_invalid_values():
    with pytest.raises(ValueError):
        normalize_pins([b"too-short"])
    with pytest.raises(ValueError):
        normalize_pins(["not-valid-hex"])
    with pytest.raises(ValueError):
        normalize_pins([])
    with pytest.raises(TypeError):
        normalize_pins([123])


async def test_matching_pin_allows_request():
    backend = AsyncPinningBackend()
    ssl_context = client_ssl_context(CERT_A, http2=False)
    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context, network_backend=backend
    ) as pool:
        response = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
        )
        assert response.status == 200
        assert response.content == b"ok"
    wait_for_settle()
    assert backend.recorder.request_count == 1


async def test_matching_pin_accepts_hex_string():
    backend = AsyncPinningBackend()
    ssl_context = client_ssl_context(CERT_A, http2=False)
    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context, network_backend=backend
    ) as pool:
        response = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_A.hex()]},
        )
        assert response.status == 200


async def test_mismatched_pin_blocks_request_before_bytes_sent():
    backend = AsyncPinningBackend()
    # Both certificates are trusted CAs; the pin is what rejects this one.
    ssl_context = client_ssl_context(CERT_A, CERT_B, http2=False)
    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context, network_backend=backend
    ) as pool:
        with pytest.raises(httpcore.CertificatePinError):
            await pool.request(
                "GET",
                "https://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_B]},
            )
    wait_for_settle()
    # The TLS handshake completed (a real certificate was presented)...
    assert backend.recorder.handshakes_completed == 1
    # ...but no business request bytes ever reached the transport.
    assert backend.recorder.request_count == 0
    # A new connection must have been established rather than the failed
    # one being reused.
    assert len(backend.streams) == 1


async def test_pin_set_with_multiple_entries():
    backend = AsyncPinningBackend()
    ssl_context = client_ssl_context(CERT_A, CERT_B, http2=False)
    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context, network_backend=backend
    ) as pool:
        response = await pool.request(
            "GET",
            "https://example.com/",
            extensions={
                "ssl_certificate_pins": [FINGERPRINT_A, FINGERPRINT_B]
            },
        )
        assert response.status == 200


async def test_non_tls_request_with_pins_is_rejected():
    backend = AsyncPinningBackend()
    async with httpcore.AsyncConnectionPool(network_backend=backend) as pool:
        with pytest.raises(httpcore.CertificatePinError):
            await pool.request(
                "GET",
                "http://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
            )
    assert backend.recorder.handshakes_completed == 0
    assert backend.recorder.request_count == 0


async def test_pin_request_directly_on_connection():
    backend = AsyncPinningBackend()
    backend._cert_rotation = [(CERT_A, backend._cert_rotation[0][1])]
    ssl_context = client_ssl_context(CERT_A, CERT_B, http2=False)
    origin = httpcore.Origin(b"https", b"example.com", 443)
    async with httpcore.AsyncHTTPConnection(
        origin=origin,
        ssl_context=ssl_context,
        network_backend=backend,
    ) as connection:
        with pytest.raises(httpcore.CertificatePinError):
            await connection.request(
                "GET",
                "https://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_B]},
            )
        # The rejected connection cannot be reused by another mismatched pin.
        with pytest.raises(httpcore.CertificatePinError):
            await connection.request(
                "GET",
                "https://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_B]},
            )
        # A matching pin on a fresh connection succeeds.
        response = await connection.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
        )
        assert response.status == 200
    wait_for_settle()
    # Two pinned requests were blocked before sending; one reached server.
    assert backend.recorder.request_count == 1


async def test_interleaved_requests_reuse_connection_only_when_pins_match():
    backend = AsyncPinningBackend()
    # Rotation is irrelevant: always present cert A.
    backend._cert_rotation = [(CERT_A, backend._cert_rotation[0][1])]
    ssl_context = client_ssl_context(CERT_A, http2=False)
    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context,
        network_backend=backend,
        max_keepalive_connections=10,
    ) as pool:
        first = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
        )
        assert first.status == 200

        # A request with a different pin must not borrow the keepalive
        # connection: it establishes a new one which fails the check and
        # sends no business bytes.
        with pytest.raises(httpcore.CertificatePinError):
            await pool.request(
                "GET",
                "https://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_B]},
            )

        # The original pinned connection is still reusable.
        again = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
        )
        assert again.status == 200
    wait_for_settle()
    # Two successful keepalive requests shared a single connection; the
    # mismatched attempt opened a second connection but sent no request.
    assert len(backend.streams) == 2
    assert backend.recorder.request_count == 2


async def test_two_rotating_certificates_interleaved_requests():
    # The backend presents cert A on the first connection and cert B on the
    # second. Both are trusted by the SSL context so ordinary CA verification
    # remains enabled, and pinning is what distinguishes them.
    backend = AsyncPinningBackend()
    ssl_context = client_ssl_context(CERT_A, CERT_B, http2=False)
    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context,
        network_backend=backend,
        max_connections=2,
        max_keepalive_connections=10,
    ) as pool:
        response_a = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
        )
        assert response_a.status == 200

        # Next backend connection presents cert B.
        response_b = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_B]},
        )
        assert response_b.status == 200

        # Reusing the keepalive pool: an A pin must land on the A connection,
        # and a B pin on the B connection.
        response = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
        )
        assert response.status == 200
        response = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_B]},
        )
        assert response.status == 200

        # A pin set containing only an unrelated fingerprint cannot use
        # either connection and must fail without sending request bytes.
        with pytest.raises(httpcore.CertificatePinError):
            await pool.request(
                "GET",
                "https://example.com/",
                extensions={"ssl_certificate_pins": [b"\xaa" * 32]},
            )
    wait_for_settle()
    # Exactly two TLS connections were established; four successful
    # requests plus the blocked one.
    assert len(backend.streams) == 3
    assert backend.recorder.request_count == 4
    assert backend.fingerprints[0] == FINGERPRINT_A
    assert backend.fingerprints[1] == FINGERPRINT_B
    assert backend.fingerprints[2] == FINGERPRINT_A


class _NoCertificateSSLObject:
    """An SSL object that a backend might expose without certificate info."""

    def selected_alpn_protocol(self) -> str:
        return "http/1.1"

    def getpeercert(self, binary_form: bool = False) -> None:
        return None


class _NoCertificateStream(httpcore.AsyncMockStream):
    async def start_tls(self, ssl_context, server_hostname=None, timeout=None):
        return self

    def get_extra_info(self, info: str):
        if info == "ssl_object":
            return _NoCertificateSSLObject()
        return None


class _NoCertificateBackend(httpcore.AsyncMockBackend):
    def __init__(self) -> None:
        super().__init__(
            [b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"]
        )

    async def connect_tcp(self, *args, **kwargs):
        return _NoCertificateStream(
            [
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok",
            ]
        )


async def test_backend_without_certificate_info_is_rejected():
    backend = _NoCertificateBackend()
    origin = httpcore.Origin(b"https", b"example.com", 443)
    async with httpcore.AsyncHTTPConnection(
        origin=origin, network_backend=backend
    ) as connection:
        with pytest.raises(httpcore.CertificatePinError):
            await connection.request(
                "GET",
                "https://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
            )


async def test_tunnel_proxy_pins_target_certificate_not_proxy():
    # HTTPS through an HTTP CONNECT proxy: the pin must match the tunnel
    # target certificate, not the (plaintext) proxy connection.
    backend = AsyncTunnelPinningBackend()
    backend._cert_rotation = [(CERT_A, backend._cert_rotation[0][1])]
    ssl_context = client_ssl_context(CERT_A, CERT_B, http2=False)
    proxy = httpcore.Proxy(url="http://proxy.example:8080/")
    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context, proxy=proxy, network_backend=backend
    ) as pool:
        response = await pool.request(
            "GET",
            "https://example.com/",
            extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
        )
        assert response.status == 200

        with pytest.raises(httpcore.CertificatePinError):
            await pool.request(
                "GET",
                "https://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_B]},
            )
    wait_for_settle()
    # Only the successful request reached the tunnel target.
    assert backend.recorder.request_count == 1


async def test_forward_http_proxy_rejects_pins_for_plain_http_target():
    backend = AsyncRawRecordingBackend()
    proxy = httpcore.Proxy(url="http://proxy.example:8080/")
    async with httpcore.AsyncConnectionPool(
        proxy=proxy, network_backend=backend
    ) as pool:
        with pytest.raises(httpcore.CertificatePinError):
            await pool.request(
                "GET",
                "http://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
            )
    assert backend.recorder.request_count == 0


async def test_http2_concurrent_requests_share_connection_with_matching_pin():
    import anyio

    backend = AsyncPinningBackend(http2=True)
    backend._cert_rotation = [(CERT_A, backend._cert_rotation[0][1])]
    ssl_context = client_ssl_context(CERT_A, http2=True)

    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context,
        network_backend=backend,
        http2=True,
        max_connections=4,
    ) as pool:

        async def fetch() -> None:
            response = await pool.request(
                "GET",
                "https://example.com/",
                extensions={"ssl_certificate_pins": [FINGERPRINT_A]},
            )
            assert response.status == 200
            assert response.content == b"ok"

        async with anyio.create_task_group() as task_group:
            for _ in range(3):
                task_group.start_soon(fetch)

    wait_for_settle()
    assert len(backend.streams) == 1
    assert backend.recorder.request_count == 3


async def test_http2_concurrent_requests_with_different_pins_use_separate_connections():
    import anyio

    backend = AsyncPinningBackend(http2=True)
    backend._cert_rotation = [(CERT_A, backend._cert_rotation[0][1])]
    ssl_context = client_ssl_context(CERT_A, CERT_B, http2=True)
    errors: list[Exception] = []

    async with httpcore.AsyncConnectionPool(
        ssl_context=ssl_context,
        network_backend=backend,
        http2=True,
        max_connections=4,
    ) as pool:

        async def fetch(pin: bytes) -> None:
            try:
                response = await pool.request(
                    "GET",
                    "https://example.com/",
                    extensions={"ssl_certificate_pins": [pin]},
                )
                assert response.status == 200
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(fetch, FINGERPRINT_A)
            task_group.start_soon(fetch, FINGERPRINT_A)
            task_group.start_soon(fetch, FINGERPRINT_B)

    wait_for_settle()
    assert len(errors) == 1
    assert isinstance(errors[0], httpcore.CertificatePinError)
    assert len(backend.streams) == 2
    assert backend.recorder.request_count == 2
