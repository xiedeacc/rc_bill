"""Summarize API-only JFR by measured wall-clock windows; nested stages are not additive."""
import collections
from datetime import datetime
import importlib.util
import json
from pathlib import Path
import re
import subprocess

spec = importlib.util.spec_from_file_location('benchmark', 'scripts/acceptance-benchmark.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)
report = json.loads(Path('ci-demo-benchmark.json').read_text())
raw = subprocess.check_output(['jfr', 'print', '--json', '--events',
    'rcbill.Stage,jdk.ExecutionSample,jdk.ThreadPark,jdk.JavaMonitorEnter,jdk.VirtualThreadPinned,jdk.SocketRead,jdk.GarbageCollection,jdk.CPULoad',
    'ci-demo-api.jfr'], text=True)
events = json.loads(raw)['recording']['events']

def seconds(duration):
    match = re.fullmatch(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?', duration)
    if not match:
        raise ValueError(duration)
    return sum(float(v or 0) * scale for v, scale in zip(match.groups(), [3600, 60, 1]))

def stack(event):
    frames = (event.get('stackTrace') or {}).get('frames', [])
    return ' <- '.join(f['method']['type']['name'] + '.' + f['method']['name'] for f in frames[:8])

result = []
for phase in report['phases']:
    stages = collections.defaultdict(list)
    waits = collections.defaultdict(list)
    samples = collections.Counter()
    cpu = []
    for event in events:
        value = event['values']
        start = datetime.fromisoformat(value['startTime']).timestamp()
        if not phase['start_epoch'] <= start <= phase['end_epoch']:
            continue
        kind = event['type']
        if kind == 'rcbill.Stage':
            stages[value['stage']].append(seconds(value['duration']) * 1000)
        elif kind == 'jdk.ExecutionSample':
            samples[stack(value)] += 1
        elif kind == 'jdk.CPULoad':
            cpu.append({k: value[k] for k in ['jvmUser', 'jvmSystem', 'machineTotal']})
        else:
            waits[kind + ': ' + stack(value)].append(seconds(value['duration']) * 1000)
    def stats(values):
        return {'count': len(values), 'total_ms': round(sum(values), 3), **benchmark.distribution(values)}
    result.append({'phase': phase['phase'], 'stages': {k: stats(v) for k, v in stages.items()},
                   'cpu_samples': cpu, 'execution_stacks': samples.most_common(15),
                   'waits': {k: stats(v) for k, v in sorted(waits.items(), key=lambda kv: -sum(kv[1]))[:15]}})
Path('ci-demo-profile.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
