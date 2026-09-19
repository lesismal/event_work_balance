# HTTP/1.x: Support, Limitations and Conformance Tests

[English](http1.md) | [简体中文](http1.zh-CN.md)

This document records what the Go `http` package supports of HTTP/1.0 and
HTTP/1.1 (RFC 9110, RFC 9112), where it stops, and how that is tested. For
usage, see the [HTTP section of the Go README](../go/README.zh-CN.md#http-子-package)
(Chinese). HTTP/2 and HTTP/3 have their own documents:
[`http2.md`](http2.md), [`http3.md`](http3.md).

## What is supported

| Area | Server | Client |
| --- | --- | --- |
| Versions | HTTP/1.0 and HTTP/1.1 requests; answers in the request's version | Sends HTTP/1.1, or HTTP/1.0 when the request's `ProtoMinor` is 0 |
| Connections | Keep-alive (HTTP/1.1 by default, HTTP/1.0 with `Connection: keep-alive`), `Connection: close` from either side, pipelining (responses in request order) | Keep-alive pool per host:port; an HTTP/1.0 connection is reused only if the request asked for keep-alive and the server agreed |
| Request bodies | `Content-Length`, chunked with extensions and trailers (`Request.Trailer`) | `Content-Length`; chunked with `req.Trailer` when `ContentLength` is -1 |
| Response bodies | `Content-Length`, chunked with trailers, close-delimited for HTTP/1.0 streams | `Content-Length`, chunked with trailers (`Response.Trailer`), close-delimited |
| Streaming | `Context` is an `http.ResponseWriter`, `http.Flusher` and `io.ReaderFrom`: `Write` + `Flush` stream the body as it is produced | Bodies are buffered whole before the callback |
| Files | `Connection.SendFile` / `Context.ReadFrom`: `sendfile(2)` on Linux and macOS, chunked reads on Windows; `http.ServeFile`, `http.ServeContent` and `http.FileServer` work through `Context` (Range, multipart ranges, conditional requests) | — |
| Interim responses | 1xx through `WriteInterim` or `WriteHeader(1xx)`, automatic `100 Continue` | 1xx skipped |
| Bodiless responses | HEAD (length of the GET kept), 204 and 1xx without `Content-Length`, 304 only with the handler's own `Content-Length` | HEAD, 204, 304 read no body |
| Validation | 400 for a missing or repeated `Host` in HTTP/1.1, for `Transfer-Encoding` in HTTP/1.0 and for malformed messages; 501 for transfer codings other than chunked; 417 for unknown expectations; 413/431 for limits | Malformed responses, unknown transfer codings and short bodies fail the request |
| Framing conflicts | `Content-Length` together with chunked: read as chunked, connection closed after the response | — |
| Headers | `Date` added unless the handler set one; the framing headers the server writes replace the handler's | — |

### Response framing chosen by the writer

When a handler uses `Write`/`WriteHeader` rather than `WriteResponse`, the
server picks the framing:

1. `Content-Length` set by the handler: identity, and writes past it fail
   with `http.ErrContentLength`; a body left short closes the connection.
2. Otherwise, a body no longer than 4KB written before the handler returns:
   `Content-Length` of what was written.
3. Otherwise, HTTP/1.1: chunked, which also carries trailers.
4. Otherwise, HTTP/1.0: the body ends when the connection closes.

`Flush` sends the header and whatever is held back and hands it to the socket
at once; without it the engine batches a read round's output until the
handler returns (see `Connection.Flush`).

### Zero-copy file sending

`Connection.SendFile(f, offset, count)` queues a file range in order with the
connection's other sends. The connection duplicates the descriptor, so the
caller may close its file at once. On Linux and macOS the bytes go from the
file to the socket by `sendfile(2)`; when the socket is full, the rest waits
for the next write edge, so the file is read only as fast as the peer
consumes it. On Linux this can be seen with
`strace -e sendfile`, for example `sendfile(10, 12, [0] => [2673856], 3145851) = 2673856`
followed by the remainder once the socket drains.

`Context.ReadFrom` uses it for regular files and for an `*io.LimitedReader`
over one (what `http.ServeContent` passes). Ranges under 16KB are copied
instead, which costs less than the extra system calls.

## Current limitations

- **Request bodies are buffered whole.** A request is handed to the handler
  once its body has arrived, bounded by `MaxBodyBytes`. Uploads cannot be
  processed as they arrive.
- **Client response bodies are buffered whole**, bounded by
  `MaxResponseBodyBytes`; the client has no streaming download.
- **The client does not pipeline**: one request per HTTP/1 connection at a
  time.
- **Streaming responses are written from the handler.** The response is ended
  when the handler returns, as in `net/http`; a handler that wants to answer
  later from another goroutine must use `WriteResponse` and not start the
  response with `Write` first.
- **Writes do not block.** A handler that writes faster than the peer reads
  queues the difference in memory (reads on the connection pause, but the
  handler is not held back). Files sent with `SendFile` are the exception:
  they are read only as the socket drains.
- **TLS cannot use sendfile.** Over TLS, `SendFile` reads the range and
  encrypts it before returning, so the whole range is queued in memory.
- **HTTP/2 and HTTP/3** accept the same `ResponseWriter` calls but hold the
  response until the handler returns (see their documents).
- **No transfer codings besides chunked** (gzip, deflate, compress) are
  decoded; such requests get 501, such responses fail.
- **No `Upgrade` handling other than h2c and WebSocket** (the latter in the
  `websocket` package); `CONNECT` requests reach the handler but cannot become
  tunnels.
- **No idle, header or body read timeouts on the server.** A slow client can
  hold a connection open; see the planned improvements.

## Planned improvements

- Server-side timeouts: read-header, body and idle, like `net/http.Server`'s
  `ReadHeaderTimeout`, `ReadTimeout` and `IdleTimeout`.
- Streaming request bodies for large uploads, and streaming response bodies
  for the client.
- A way for a handler to wait for its queued output to drain, so that
  streaming a large generated body does not grow memory.
- Lazily encrypted file sending over TLS, reading the file only as the socket
  drains, and kTLS on Linux for true zero-copy HTTPS.
- `TransmitFile` on Windows.

## Conformance tests

`go/http/http1_conformance_test.go` (every test is named
`TestHTTP1Conformance…`) checks the fib server and client against peers that
are not fib:

- **fib server** against Go's `net/http` client, raw TCP connections (for
  requests `net/http` never sends: HTTP/1.0 with and without keep-alive,
  pipelined requests, malformed and conflicting framing, chunk extensions),
  and **curl** (`--http1.0`, keep-alive reuse, chunked upload, downloads of the
  sendfile path, ranges, raw chunked trailers).
- **fib client** against Go's `net/http` server (`httptest`) and raw servers
  (HTTP/1.0 keep-alive and close-delimited responses, malformed responses).
- **fib client against fib server**, including HTTP/1.0 and sendfile.
- `go/sendfile_test.go` checks `Connection.SendFile` over TCP and Unix
  sockets, from the handler and from other goroutines, with a slow reader, and
  a file shorter than its range.

The peers are the standard library and the curl binary, so the Go module takes
no new dependency. CI runs the suite as its own job, **HTTP/1.x conformance**,
on Linux, macOS and Windows with `FIB_REQUIRE_CURL=1`, which makes a missing
curl fail the job instead of skipping those cases. To run it locally:

```sh
cd go
go test -race -run 'TestHTTP1Conformance|TestSendFile|TestSendableFileOf|TestResponseWriter' -v . ./http/
```
