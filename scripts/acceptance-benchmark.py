"""Bounded closed-loop acceptance experiment. Real ledger; no production capacity claim."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import platform
import os
import time
import urllib.error
import urllib.request
import uuid


def distribution(values):
    if not values:
        raise ValueError('No latency samples')
    values = sorted(values)
    return {**{f'p{p}_ms': round(values[math.ceil(len(values) * p / 100) - 1], 3)
               for p in [50, 95, 99]}, 'max_ms': round(values[-1], 3)}


def summarize(rows, seconds):
    accepted = sum(r['status'] == 202 for r in rows)
    return {**distribution([r['latency_ms'] for r in rows]), 'requests': len(rows),
            'accepted': accepted, 'errors': len(rows) - accepted,
            'accepted_per_second': round(accepted / seconds, 3), 'seconds': round(seconds, 3)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='ci-demo-benchmark.json')
    args = parser.parse_args()
    token = Path('tmp/demo/token.txt').read_text().strip()
    body = json.dumps({'destinationId': 'demo-supplier', 'destinationVersion': 1,
                       'contentType': 'text/plain', 'body': 'x' * 2048, 'ttlSeconds': 60}).encode()
    prefix = uuid.uuid4().hex

    def call(path, payload=None, key=None):
        headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}
        if key:
            headers['Idempotency-Key'] = key
        req = urllib.request.Request('http://127.0.0.1:8080' + path, data=payload, headers=headers)
        try:
            response = urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as failure:
            response = failure
        with response:
            return response.status, json.loads(response.read())

    report = {'protocol': 1, 'concurrency': 8, 'body_bytes': 2048, 'capacity_claim': False,
              'connection_policy': 'urllib-new-connection-per-request',
              'host': {'cpu_count': os.cpu_count(), 'platform': platform.platform()}, 'phases': []}
    failed = False
    try:
        for phase, count in [('cold', 64), ('warmup', 256), ('steady-1', 512), ('steady-2', 512), ('steady-3', 512)]:
            def submit(index):
                started = time.perf_counter()
                wall = time.time()
                try:
                    status, response = call('/v1/notifications', body, f'{prefix}-{phase}-{index}')
                    task = response.get('notificationId')
                    code = response.get('code')
                except Exception as failure:
                    status, task, code = 0, None, type(failure).__name__
                return {'index': index, 'start_epoch': wall, 'status': status, 'task': task,
                        'code': code, 'latency_ms': (time.perf_counter() - started) * 1000}

            start_epoch, started = time.time(), time.perf_counter()
            with ThreadPoolExecutor(max_workers=8) as executor:
                rows = list(executor.map(submit, range(count)))
            elapsed, end_epoch = time.perf_counter() - started, time.time()
            pending = {r['task'] for r in rows if r['status'] == 202 and r['task']}
            terminal_errors = 0
            deadline = time.monotonic() + 90
            while pending and time.monotonic() < deadline:
                for task in list(pending):
                    status, result = call('/v1/notifications/' + task)
                    if status != 200:
                        raise RuntimeError(f'Status lookup returned {status}')
                    if result['status'] in {'SUCCEEDED', 'DEAD', 'EXPIRED'}:
                        pending.remove(task)
                        terminal_errors += result['status'] != 'SUCCEEDED'
                if pending:
                    time.sleep(.2)
            summary = summarize(rows, elapsed)
            summary.update({'phase': phase, 'start_epoch': start_epoch, 'end_epoch': end_epoch,
                            'unfinished': len(pending), 'terminal_errors': terminal_errors,
                            'samples': [{k: v for k, v in r.items() if k != 'task'} for r in rows]})
            report['phases'].append(summary)
            print(json.dumps({k: v for k, v in summary.items() if k != 'samples'}), flush=True)
            failed |= bool(summary['errors'] or pending or terminal_errors)
            if failed:
                break
    finally:
        Path(args.output).write_text(json.dumps(report, indent=2), encoding='utf-8')
    if failed:
        raise SystemExit('Benchmark correctness gate failed; see raw evidence.')


if __name__ == '__main__':
    main()
