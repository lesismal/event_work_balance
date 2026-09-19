#!/usr/bin/env bash
# Runs the Autobahn fuzzingclient against the echo server in several short
# wstest processes instead of one, writing each one's report under
# <reports>/<group>, then checks every report.
#
# The compression cases (12.*, 13.*) are split by subgroup because the test
# client, not the server, is what grows: with client_no_context_takeover it
# creates a zlib compressor for every message, and PyPy frees their memory
# only on a major collection. One process running all of them grew past 3 GiB
# and was killed on a CI runner.
#
# Usage: run.sh [reports-dir]
# AUTOBAHN_URL (default ws://127.0.0.1:9001) is the server to test, and
# AUTOBAHN_NETWORK (default host) the Docker network the client joins.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
reports=$(mkdir -p "${1:-reports}" && cd "${1:-reports}" && pwd)
url=${AUTOBAHN_URL:-ws://127.0.0.1:9001}
network=${AUTOBAHN_NETWORK:-host}
config=$(mktemp -d)
trap 'rm -rf "$config"' EXIT

groups=(core)
for sub in 1 2 3 4 5; do groups+=("12.$sub"); done
for sub in 1 2 3 4 5 6 7; do groups+=("13.$sub"); done

for group in "${groups[@]}"; do
  if [ "$group" = core ]; then
    cases='["*"]'
    exclude='["12.*", "13.*"]'
  else
    cases="[\"$group.*\"]"
    exclude='[]'
  fi
  cat > "$config/$group.json" <<EOF
{
  "outdir": "/reports/$group",
  "servers": [{"agent": "fib", "url": "$url"}],
  "cases": $cases,
  "exclude-cases": $exclude,
  "exclude-agent-cases": {}
}
EOF
  echo "::group::Autobahn $group"
  docker run --rm --network "$network" \
    -v "$config:/config" \
    -v "$reports:/reports" \
    crossbario/autobahn-testsuite \
    wstest -m fuzzingclient -s "/config/$group.json"
  echo "::endgroup::"
done

python3 "$here/check_report.py" "$reports"/*/index.json
