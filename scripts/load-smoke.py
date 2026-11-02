"""Small functional concurrency smoke; not a 100k-QPS benchmark. Loopback targets only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse
import uuid

parser = argparse.ArgumentParser()
parser.add_argument('--url', default='http://127.0.0.1:8080')
parser.add_argument('--token-file', default='tmp/demo/token.txt')
parser.add_argument('--requests', type=int, default=64)
parser.add_argument('--concurrency', type=int, default=8)
args = parser.parse_args()
if urlparse(args.url).hostname not in {'127.0.0.1', 'localhost', '::1'} or not 1 <= args.requests <= 10000 or not 1 <= args.concurrency <= 256:
    raise SystemExit('Use a loopback fixture and bounded request/concurrency counts.')
token = Path(args.token_file).read_text().strip()
prefix = 'load-' + uuid.uuid4().hex
def call(path, body=None, key=None):
    headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
    if key:
        headers['Idempotency-Key'] = key
    request = urllib.request.Request(args.url + path, data=body, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, json.loads(response.read())
def submit(index):
    start = time.monotonic()
    body = json.dumps({'destinationId': 'demo-supplier', 'destinationVersion': 1,
                       'contentType': 'text/plain', 'body': 'x' * 2048, 'ttlSeconds': 60}).encode()
    status, result = call('/v1/notifications', body, prefix + '-' + str(index))
    if status != 202:
        raise RuntimeError(f'Expected durable acceptance; got {status}: {result.get("code")}')
    return result['notificationId'], (time.monotonic() - start) * 1000
started = time.monotonic()
with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
    accepted = list(pool.map(submit, range(args.requests)))
pending = {task for task, _ in accepted}
deadline = time.monotonic() + 45
while pending and time.monotonic() < deadline:
    for task in list(pending):
        status, result = call('/v1/notifications/' + task)
        if status != 200:
            raise RuntimeError(f'Status lookup failed: {status}')
        if result['status'] == 'SUCCEEDED':
            pending.remove(task)
        elif result['status'] in {'DEAD', 'EXPIRED'}:
            raise RuntimeError(f'Task ended in {result["status"]}')
    if pending:
        time.sleep(0.2)
if pending:
    raise SystemExit(f'{len(pending)} tasks did not finish within the smoke deadline.')
latencies = sorted(value for _, value in accepted)
print(json.dumps({'requests': args.requests, 'concurrency': args.concurrency, 'succeeded': len(accepted),
                  'acceptance_p95_ms': round(latencies[math.ceil(len(latencies) * .95) - 1], 2),
                  'total_seconds': round(time.monotonic() - started, 2), 'capacity_claim': False}, indent=2))
