#!/usr/bin/env bash
set -euo pipefail
cleanup() {
  docker logs rc-bill-api-image > ci-image-api.log 2>&1 || true
  docker logs rc-bill-worker-image > ci-image-worker.log 2>&1 || true
  docker rm -f rc-bill-api-image rc-bill-worker-image >/dev/null 2>&1 || true
}
trap cleanup EXIT
for role in api worker; do
  test "$(docker image inspect "rc-bill-$role:verified" --format '{{.Config.User}}')" = '10001:10001'
  port=18080
  if [ "$role" = worker ]; then port=18081; fi
  docker run -d --name "rc-bill-$role-image" --network host --memory=512m --cpus=1 \
    --read-only --cap-drop=ALL --tmpfs /tmp:rw,nosuid,size=64m \
    "rc-bill-$role:verified" --spring.profiles.active=scaffold --server.port="$port"
  ready=0
  for attempt in $(seq 1 60); do
    if curl -fsS "http://127.0.0.1:$port/actuator/health/liveness" >/dev/null; then ready=1; break; fi
    sleep 1
  done
  test "$ready" = 1
  test "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/actuator/health/readiness")" = 503
done
test "$(curl -s -o /dev/null -w '%{http_code}' -X POST http://127.0.0.1:18080/v1/notifications)" = 501
echo 'Both read-only non-root images passed local scaffold smoke checks.'
