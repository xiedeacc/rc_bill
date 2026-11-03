#!/usr/bin/env bash
set -euo pipefail
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT
python=.ci-python/bin/python
"$python" scripts/prepare-demo.py
"$python" scripts/run-demo.py migrate > ci-demo-migration.log 2>&1
"$python" scripts/run-demo.py bootstrap > ci-demo-bootstrap.log 2>&1
"$python" scripts/mock-external-services.py > ci-demo-mocks.log 2>&1 & pids+=("$!")
JAVA_TOOL_OPTIONS='-XX:StartFlightRecording=settings=profile,filename=ci-demo-api.jfr,dumponexit=true,maxsize=128m' "$python" scripts/run-demo.py api > ci-demo-api.log 2>&1 & pids+=("$!")
"$python" scripts/run-demo.py worker > ci-demo-worker.log 2>&1 & pids+=("$!")
for port in 8080 8081; do
  ready=0
  for attempt in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$port/actuator/health/readiness" >/dev/null; then ready=1; break; fi
    sleep 1
  done
  test "$ready" = 1
done
"$python" scripts/acceptance-benchmark.py
cleanup
pids=()
"$python" scripts/summarize-acceptance-jfr.py
