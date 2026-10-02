from __future__ import annotations

import hashlib
import typing

from ._exceptions import CertificatePinError
from ._models import Request

# Per-request extension key. The value is a collection of SHA-256 certificate
# fingerprints, each given either as a raw 32 byte digest, or as 64 hex
# characters.
SSL_CERTIFICATE_PINS = "ssl_certificate_pins"

CertificatePin = typing.Union[bytes, bytearray, str]
CertificatePins = typing.Collection[CertificatePin]


def normalize_pins(pins: CertificatePins) -> frozenset[bytes]:
    """
    Normalize a collection of certificate pins into a set of raw 32 byte
    SHA-256 digests.

    Pins may be provided either as raw bytes, or as hex encoded strings.
    """
    normalized: set[bytes] = set()
    for pin in pins:
        if isinstance(pin, str):
            try:
                digest = bytes.fromhex(pin)
            except ValueError:
                raise ValueError(
                    "Invalid certificate pin. Expected either 32 raw bytes or "
                    f"64 hex characters, but got {pin!r}."
                )
        elif isinstance(pin, (bytes, bytearray)):
            value = bytes(pin)
            if len(value) == 64:
                try:
                    digest = bytes.fromhex(value.decode("ascii"))
                except (ValueError, UnicodeDecodeError):
                    digest = value
            else:
                digest = value
        else:
            seen_type = type(pin).__name__
            raise TypeError(
                "Certificate pins must be bytes or str, but got "
                f"{seen_type}."
            )

        if len(digest) != 32:
            raise ValueError(
                "Invalid certificate pin. Expected a SHA-256 digest of 32 "
                f"bytes, but got {len(digest)} bytes: {pin!r}."
            )
        normalized.add(digest)

    if not normalized:
        raise ValueError(
            "An empty collection of certificate pins does not match any "
            "certificate."
        )

    return frozenset(normalized)


def get_request_pins(request: Request) -> frozenset[bytes] | None:
    """
    Return the normalized set of certificate pins configured on the request,
    or `None` if the request does not use certificate pinning.
    """
    if SSL_CERTIFICATE_PINS not in request.extensions:
        return None
    return normalize_pins(request.extensions[SSL_CERTIFICATE_PINS])


def get_peer_certificate_fingerprint(ssl_object: typing.Any) -> bytes | None:
    """
    Return the SHA-256 fingerprint of the complete DER encoded peer
    certificate, or `None` if the backend does not expose certificate
    information.
    """
    try:
        der_certificate = ssl_object.getpeercert(binary_form=True)
    except AttributeError:
        return None
    if der_certificate is None:
        return None
    return hashlib.sha256(der_certificate).digest()


def enforce_certificate_pins(
    ssl_object: typing.Any, pins: frozenset[bytes] | None
) -> bytes | None:
    """
    Verify that the presented peer certificate matches one of the pinned
    SHA-256 fingerprints. Called after the TLS handshake has completed, but
    before any HTTP request bytes are sent.

    Returns the peer certificate fingerprint, or `None` if pinning is not
    in use.
    """
    if pins is None:
        return None

    fingerprint = (
        None
        if ssl_object is None
        else get_peer_certificate_fingerprint(ssl_object)
    )
    if fingerprint is None:
        raise CertificatePinError(
            "Certificate pinning was requested, but the network backend "
            "could not provide the server's DER encoded certificate."
        )
    if fingerprint not in pins:
        raise CertificatePinError(
            "Server certificate fingerprint "
            f"{fingerprint.hex()} does not match any pinned certificate."
        )
    return fingerprint
