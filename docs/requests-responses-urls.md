# Requests, Responses, and URLs

TODO

## Requests

Request instances in `httpcore` are deliberately simple, and only include the essential information required to represent an HTTP request.

Properties on the request are plain byte-wise representations.

```python
>>> request = httpcore.Request("GET", "https://www.example.com/")
>>> request.method
b"GET"
>>> request.url
httpcore.URL(scheme=b"https", host=b"www.example.com", port=None, target=b"/")
>>> request.headers
[(b'Host', b'www.example.com')]
>>> request.stream
<httpcore.ByteStream [0 bytes]>
```

The interface is liberal in the types that it accepts, but specific in the properties that it uses to represent them. For example, headers may be specified as a dictionary of strings, but internally are represented as a list of `(byte, byte)` tuples.

```python
>>> headers = {"User-Agent": "custom"}
>>> request = httpcore.Request("GET", "https://www.example.com/", headers=headers)
>>> request.headers
[(b'Host', b'www.example.com'), (b"User-Agent", b"custom")]

## Responses

...

HTTP/1.1 chunked responses and HTTP/2 responses may include trailer fields
after the response body. Access `response.trailers` without reading any
additional data:

```python
with httpcore.stream("GET", "https://www.example.com/") as response:
    body = response.read()
    trailers = response.trailers
```

`trailers` is `None` until the response body has reached its terminator. This
includes the initial-headers phase, an early close, and a protocol error while
reading the terminator. Once the complete response has been read, repeated
queries return the same list. A response without trailer fields results in an
empty list.

The trailer list is a sequence of `(bytes, bytes)` pairs and preserves the
field order and repeated values sent by the peer. Trailer fields are never
included in `response.headers`, and accessing `response.trailers` does not
read or buffer the remaining response body.

Fields that must not be trailer fields are rejected consistently on HTTP/1.1
and HTTP/2 with `httpcore.RemoteProtocolError`.

## URLs

...

---

# Reference

## `httpcore.Request`

::: httpcore.Request
    handler: python
    rendering:
        show_source: False

## `httpcore.Response`

::: httpcore.Response
    handler: python
    rendering:
        show_source: False

## `httpcore.URL`

::: httpcore.URL
    handler: python
    rendering:
        show_source: False
