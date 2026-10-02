from __future__ import annotations

import hashlib
import typing

from ._exceptions import CertificatePinError


SHA256_DIGEST_SIZE = hashlib.sha256().digest_size


def get_ssl_pins(request: typing.Any) -> frozenset[bytes] | None:
    pins = request.extensions.get("ssl_pins")
    if pins is None:
        return None

    try:
        normalized = frozenset(pins)
    except TypeError as exc:
        raise TypeError("ssl_pins must be an iterable of SHA-256 digests.") from exc

    if not normalized:
        raise ValueError("ssl_pins must not be empty.")

    for pin in normalized:
        if not isinstance(pin, bytes) or len(pin) != SHA256_DIGEST_SIZE:
            raise ValueError(
                "ssl_pins must contain 32 byte SHA-256 certificate digests."
            )

    return normalized


def require_tls_request(request: typing.Any) -> None:
    if get_ssl_pins(request) is not None and request.url.scheme not in (
        b"https",
        b"wss",
    ):
        raise CertificatePinError(
            "Certificate pinning is only supported for TLS requests."
        )


def get_peer_certificate_sha256(ssl_object: typing.Any) -> bytes:
    if ssl_object is None:
        raise CertificatePinError(
            "Certificate pinning requires a network backend that provides an SSL object."
        )

    getpeercert = getattr(ssl_object, "getpeercert", None)
    if not callable(getpeercert):
        raise CertificatePinError(
            "Certificate pinning requires an SSL object that exposes the peer certificate."
        )

    der = getpeercert(binary_form=True)
    if not isinstance(der, bytes) or not der:
        raise CertificatePinError("Unable to obtain the peer certificate in DER form.")

    return hashlib.sha256(der).digest()


def connection_accepts_ssl_pins(
    connection_pins: frozenset[bytes] | None,
    fingerprint: bytes | None,
    request_pins: frozenset[bytes] | None,
) -> bool:
    if request_pins is None:
        return True
    if fingerprint is not None:
        return fingerprint in request_pins
    return connection_pins is not None and bool(connection_pins & request_pins)


def combine_ssl_pins(
    connection_pins: frozenset[bytes] | None,
    request_pins: frozenset[bytes] | None,
) -> frozenset[bytes] | None:
    if connection_pins is None:
        return request_pins
    if request_pins is None:
        return connection_pins
    return connection_pins & request_pins


def ssl_pins_match(
    fingerprint: bytes,
    pins: frozenset[bytes] | None,
) -> bool:
    return pins is None or fingerprint in pins
