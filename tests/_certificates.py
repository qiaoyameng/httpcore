"""
Shared certificate material for the certificate pinning tests.

The tests perform real TLS handshakes (using `ssl.SSLObject` on top of
in-memory byte queues), so that the fingerprint checked by httpcore is a
genuine SHA-256 digest of the complete DER encoded peer certificate.

Two distinct self-signed end-entity certificates are generated for the
DNS name "example.com" (they are written to `tests/certificates/` using
the OpenSSL CLI). Both are loaded as trusted CAs by the test client, so
normal certificate authority and hostname verification stays enabled and
it is the pin set that distinguishes the two servers.
"""
from __future__ import annotations

import hashlib
import os
import ssl
import subprocess

CERTIFICATES_DIR = os.path.join(os.path.dirname(__file__), "certificates")

CERT_A = os.path.join(CERTIFICATES_DIR, "certA.pem")
KEY_A = os.path.join(CERTIFICATES_DIR, "keyA.pem")
CERT_B = os.path.join(CERTIFICATES_DIR, "certB.pem")
KEY_B = os.path.join(CERTIFICATES_DIR, "keyB.pem")


def _generate_certificate(cert_path: str, key_path: str) -> None:
    os.makedirs(CERTIFICATES_DIR, exist_ok=True)
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key_path,
            "-out",
            cert_path,
            "-days",
            "2",
            "-nodes",
            "-subj",
            "/CN=example.com",
            "-addext",
            "subjectAltName=DNS:example.com",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _ensure_certificates() -> None:
    for cert_path, key_path in ((CERT_A, KEY_A), (CERT_B, KEY_B)):
        if not os.path.exists(cert_path) or not os.path.exists(key_path):
            _generate_certificate(cert_path, key_path)


def server_ssl_context(cert_path: str, key_path: str, http2: bool) -> ssl.SSLContext:
    _ensure_certificates()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    context.set_alpn_protocols(["h2", "http/1.1"] if http2 else ["http/1.1"])
    return context


def client_ssl_context(*cert_paths: str, http2: bool) -> ssl.SSLContext:
    _ensure_certificates()
    context = ssl.create_default_context()
    for cert_path in cert_paths:
        context.load_verify_locations(cert_path)
    context.set_alpn_protocols(["h2", "http/1.1"] if http2 else ["http/1.1"])
    return context


def fingerprint(cert_path: str) -> bytes:
    _ensure_certificates()
    with open(cert_path) as handle:
        der_certificate = ssl.PEM_cert_to_DER_cert(handle.read())
    return hashlib.sha256(der_certificate).digest()


_ensure_certificates()
FINGERPRINT_A = fingerprint(CERT_A)
FINGERPRINT_B = fingerprint(CERT_B)
