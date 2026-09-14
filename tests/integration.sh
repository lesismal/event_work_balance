#!/bin/sh
set -eu

port=19090
./echo_server "$port" >/tmp/epoll_echo_test.log 2>&1 &
server_pid=$!
trap 'kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true' EXIT INT TERM

python3 - "$port" <<'PY'
import concurrent.futures
import socket
import sys
import time

port = int(sys.argv[1])
for _ in range(100):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=.1):
            break
    except OSError:
        time.sleep(.02)
else:
    raise SystemExit("server did not start")

def exchange(index):
    payload = (("connection-%d:" % index).encode() + bytes(range(256))) * 256
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(payload)
        received = bytearray()
        while len(received) < len(payload):
            block = sock.recv(65536)
            if not block:
                raise RuntimeError("unexpected EOF")
            received.extend(block)
    if received != payload:
        raise RuntimeError("echo mismatch")

with concurrent.futures.ThreadPoolExecutor(max_workers=24) as executor:
    list(executor.map(exchange, range(96)))
print("integration test passed: 96 concurrent ET connections")
PY
