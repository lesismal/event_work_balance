# HTTP/2: Limitations, Intentional Omissions and Planned Improvements

[English](http2.md) | [简体中文](http2.zh-CN.md)

This document records the boundaries of the HTTP/2 implementation in the Go
`http` package: how it currently behaves in ways users need to know about,
what was deliberately left out, and where it can be improved. For usage, see
the [HTTP/2 section of the Go README](../go/README.zh-CN.md#http2) (Chinese).

The implementation lives in [`go/http`](../go/http) (`h2_*.go`) and an
internal hpack package. It is written from scratch and does not depend on
`golang.org/x/net`.

## What is supported (overview)

| Side | Features |
| --- | --- |
| Server | TLS + ALPN (h2), cleartext with prior knowledge (h2c), HTTP/1.1 `Upgrade: h2c`; multiplexing, flow control in both directions, HPACK (Huffman and dynamic table), CONTINUATION, trailers, server push, 1xx interim responses, automatic 100 Continue, graceful GOAWAY on `Response.Close`, `Request.TLS` |
| Client | h2 over https through ALPN, cleartext prior knowledge with `UnencryptedHTTP2`; multiplexing on one connection, honouring the server's `MAX_CONCURRENT_STREAMS`, cancellation that resets only its own stream, automatic retry after GOAWAY / REFUSED_STREAM |
| Interop | Go `net/http` client and server (TLS and h2c); curl (nghttp2) over h2, h2c and Upgrade |

## Current limitations

Behaviour to be aware of when using it.

### Bodies are buffered whole; no streaming

- A request body is read completely into memory before the handler runs, and a
  response is given all at once as `Response.Body []byte`. The client likewise
  buffers the whole response body before its callback.
- A handler may write its response through `Context`'s `http.ResponseWriter`
  methods (`Header`/`WriteHeader`/`Write`/`Flush`), trailers included, and
  hand `Context` to `http.ServeFile` or `http.ServeContent`. On HTTP/1 that
  streams, chunked, with files sent by sendfile (see [`http1.md`](http1.md));
  on HTTP/2 the response is held until the handler returns and then sent
  whole, and `Flush` does nothing.
- Server-sent events, streamed long-polling output, gRPC streaming and
  processing large uploads as they arrive are therefore not possible over
  HTTP/2.
- Memory bound: on the server, one connection can hold up to about
  `MaxConcurrentStreams × MaxBodyBytes` (250 × 16MB by default); on the client,
  one response is bounded by `MaxResponseBodyBytes`.
- A `CONNECT` request is parsed and handed to the handler, but no tunnel can be
  established (there is no bidirectional streaming channel).

### Handlers run synchronously on the connection's worker

- A connection's frames are processed in order, and a handler is called
  synchronously on the worker reading the connection once a request is
  complete. A handler that blocks holds up the other streams on the same
  connection (application-level head-of-line blocking).
- For slow work, start a goroutine in the handler and call
  `Respond`/`WriteResponse` from it; responses on different streams do not
  block each other.
- `Push` runs the handler of the pushed request synchronously and returns only
  when it does, so a slow pushed handler delays the parent response.

### No direct writes to the connection

- Do not call `Context.Conn.Send` or similar on an HTTP/2 connection: raw bytes
  break the framing. All output must go through `Context`.

### Fixed parameters

These are constants today and cannot be configured:

| Parameter | Value |
| --- | --- |
| Receive window per stream | 1MB, replenished after half is consumed |
| Receive window per connection | 16MB, replenished after half is consumed |
| Advertised `SETTINGS_MAX_FRAME_SIZE` | 16384 |
| HPACK dynamic table | 4096 bytes (the encoder never uses more than 4096) |
| Concurrent streams the client assumes before the server's SETTINGS arrive | 100 |

### Protocol details

- **Priority**: PRIORITY frames and the priority fields in HEADERS are validated
  and ignored. When several streams have data waiting, the order they are sent
  in follows the iteration order of an internal map, with no fairness or
  weighting.
- **The peer's `SETTINGS_MAX_HEADER_LIST_SIZE`**: this side advertises its own
  limit but does not check the peer's when sending.
- **Request trailers**: the client cannot send request trailers (both sides
  can receive trailers, and the server sends response trailers).
- **TCP RST on graceful close**: after GOAWAY the connection closes once every
  stream has finished, but it does not half-close and drain what the peer is
  still sending first; if unread data is left in the socket, the kernel sends a
  RST.
- **Engine shutdown**: `Engine.Stop`/`Close` sends no GOAWAY to HTTP/2
  connections; they are closed outright, and clients see a broken connection
  rather than a graceful close.
- **Idle and keep-alive**: the server has no idle timeout for HTTP/2
  connections and sends no PING keep-alives. The client only has
  `IdleConnTimeout` (closing when idle) and does no PING health checks, so a
  connection that dies silently is not noticed promptly.
- **SETTINGS acknowledgement timeout**: nothing checks that the peer
  acknowledges this side's SETTINGS in reasonable time (SETTINGS_TIMEOUT).

### Client

- Until the first connection to an https host has revealed its protocol, only
  one connection to it is dialed at a time (as in `net/http`), so the first
  burst of requests to an HTTP/1.1-only https server waits one extra TLS
  handshake.
- Among several HTTP/2 connections, the first one with room is chosen rather
  than the least loaded one.
- A request whose connection breaks after part of its response has arrived
  fails and is not retried. Only requests that received nothing and use an
  idempotent method, and requests GOAWAY or REFUSED_STREAM marks as
  unprocessed, are retried.
- A request with `Expect: 100-continue` does not wait for the 100; the body is
  sent along with the header (which the protocol allows).
- Cleartext HTTP/2 is prior knowledge only; the client does not upgrade from
  HTTP/1.1 with `Upgrade: h2c`.

## Intentionally not implemented

| Feature | Reason |
| --- | --- |
| Receiving server push in the client | Chrome and Firefox have removed push, and Go's `net/http` client never supported it. The client sends `ENABLE_PUSH=0`; the server's push support remains for clients that still want it. For preloading, 103 Early Hints (`Context.WriteInterim`) is the recommended replacement. |
| RFC 9218 extensible priorities (`priority` header, PRIORITY_UPDATE) | With bodies buffered whole and handlers run synchronously, scheduling has little to gain. The RFC 7540 priority tree is deprecated by RFC 9113 and is not implemented either. |
| Extended CONNECT (RFC 8441, WebSocket over HTTP/2) | Needs bidirectional streaming streams, which need streaming bodies first; WebSocket uses the HTTP/1.1 Upgrade. |
| `Upgrade: h2c` initiated by the client | RFC 9113 deprecates this upgrade; cleartext HTTP/2 uses prior knowledge (`UnencryptedHTTP2`). The server still accepts the upgrade for compatibility with curl and others. |
| `Upgrade: h2c` over TLS | The RFCs allow switching protocols over TLS only through ALPN. |
| 1xx interim responses to HTTP/1.0 requests | HTTP/1.0 clients do not understand 1xx; `WriteInterim` returns `http.ErrNotSupported`. |
| Pushing from a pushed request | The protocol only allows PUSH_PROMISE on client-initiated streams. |

## Planned improvements

In order of priority.

### 1. Hardening against abuse (high)

Resources are bounded today only by `MaxConcurrentStreams`, `MaxHeaderBytes`,
`MaxBodyBytes` and the flow-control windows. Against a malicious client it
still lacks:

- **Rapid Reset (CVE-2023-44487)**: a client can open streams and immediately
  RST_STREAM them without end. Resets per unit of time should be counted, with
  GOAWAY(ENHANCE_YOUR_CALM) past a threshold.
- **Control-frame floods**: PING, SETTINGS, empty DATA and WINDOW_UPDATE frames
  are not rate-limited, and PING/SETTINGS acknowledgements keep entering the
  send queue. The number of queued control frames should be capped.
- **CONTINUATION floods**: a single header block is bounded by
  `MaxHeaderBytes`, but the number of CONTINUATION frames, and of zero-length
  frames, should be bounded too.
- **Slow connections**: with no header-read timeout or idle timeout (see
  above), many half-open connections can tie up resources.

### 2. Conformance testing (high)

- Add [h2spec](https://github.com/summerwind/h2spec) to CI, as Autobahn is for
  WebSocket, to verify RFC 9113 / RFC 7541 conformance continuously.
- Today's verification relies on interop tests against Go `net/http` and curl
  plus hand-written raw-frame tests, which cannot cover every error path.

### 3. Streaming bodies and the handler model (medium)

- Make the `http.ResponseWriter` methods `Context` already has stream on
  HTTP/2 as they do on HTTP/1, and offer streaming request-body reads, for SSE,
  gRPC and large transfers. Window updates could then follow actual
  consumption, giving real end-to-end backpressure instead of today's
  replenish-on-receipt.

### 4. Performance (medium)

- **HPACK Huffman decoding** walks the decoding tree bit by bit; table-driven
  decoding 4 or 8 bits at a time would be faster.
- **HPACK encoder** searches the dynamic table linearly; an index would help
  (the static table is already a map).
- **Allocations**: every request allocates a header map, frame buffers, an
  `stdhttp.Request` and more. Frame and header-decoding buffers could be
  recycled with `sync.Pool`, as the HTTP/1 path does.
- **Batching sends**: control frames produced in one pass (WINDOW_UPDATE,
  SETTINGS ACK, PING ACK) are sent one by one and could be coalesced into one
  write.
- **Benchmarks**: add HTTP/2 benchmarks (multiplexed throughput on one
  connection, HPACK encode/decode) and let pprof guide optimisation.

### 5. Configurability and scheduling (low)

- Make the receive windows, maximum frame size, HPACK table size and the
  client's initial stream allowance configurable; optionally size windows from
  the bandwidth-delay product.
- Schedule streams with pending data round-robin or by weight, for fairness.
- Honour the peer's `SETTINGS_MAX_HEADER_LIST_SIZE` when sending.
- Server: configurable idle timeout and PING keep-alive; GOAWAY to HTTP/2
  connections when the `Engine` stops, waiting for in-flight streams (graceful
  shutdown); half-close and drain input after GOAWAY to avoid RST.
- Client: optional PING health checks, least-loaded connection selection, and
  a SETTINGS acknowledgement timeout.
- Support sending request trailers.
