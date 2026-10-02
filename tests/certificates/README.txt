Test certificates used by the certificate pinning tests.

Generated with OpenSSL as self-signed end-entity certificates for the
DNS name "example.com":

    openssl req -x509 -newkey rsa:2048 -keyout keyX.pem -out certX.pem \
        -days 2 -nodes -subj "/CN=example.com" \
        -addext "subjectAltName=DNS:example.com"

The key material is only ever exchanged over in-memory TLS streams
inside the test process.
