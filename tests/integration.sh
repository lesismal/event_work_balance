#!/bin/sh
set -eu

server_pid=
trap 'if [ -n "$server_pid" ]; then kill "$server_pid" 2>/dev/null || true; wait "$server_pid" 2>/dev/null || true; fi' EXIT INT TERM

run_case() {
    port=$1
    use_writev=$2
    ./echo_server "$port" "$use_writev" >/tmp/epoll_echo_test.log 2>&1 &
    server_pid=$!

    python3 - "$port" "$use_writev" <<'PY'
import concurrent.futures
import socket
import sys
import time

port = int(sys.argv[1])
mode = "writev" if int(sys.argv[2]) else "write"
for _ in range(100):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=.1):
            break
    except OSError:
        time.sleep(.02)
else:
    raise SystemExit("server did not start")

with socket.create_connection(("127.0.0.1", port), timeout=5) as urgent:
    urgent.send(b"!", socket.MSG_OOB)
    if urgent.recv(1) != b"!":
        raise RuntimeError("urgent-data echo mismatch")

def exchange(index):
    payload = (("connection-%d:" % index).encode() + bytes(range(256))) * 16384
    with socket.create_connection(("127.0.0.1", port), timeout=15) as sock:
        sock.sendall(payload)
        time.sleep(.02)  # Encourage send-buffer backpressure and EPOLLOUT.
        received = bytearray()
        while len(received) < len(payload):
            block = sock.recv(65536)
            if not block:
                raise RuntimeError("unexpected EOF")
            received.extend(block)
    if received != payload:
        raise RuntimeError("echo mismatch")

with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    list(executor.map(exchange, range(16)))
print("integration test passed: 16 concurrent backpressured ET connections (%s)" % mode)
PY

    kill "$server_pid"
    wait "$server_pid"
    server_pid=
}

run_case 19090 0
run_case 19091 1
