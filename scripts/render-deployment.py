"""Render only digest-pinned release images; retain all non-image manifest fields."""
import argparse
from pathlib import Path
import re
import yaml

parser = argparse.ArgumentParser()
parser.add_argument('--api', required=True)
parser.add_argument('--worker', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()
for role, image in [('api', args.api), ('worker', args.worker)]:
    if not re.fullmatch(r'ghcr\.io/[a-z0-9_.-]+/rc-bill-' + role + r'@sha256:[a-f0-9]{64}', image):
        raise SystemExit(f'{role} image must be a verified GHCR digest')
out = Path(args.output)
out.mkdir(parents=True, exist_ok=True)
for source in Path('deploy/k8s').glob('*.yaml'):
    docs = list(yaml.safe_load_all(source.read_text(encoding='utf-8')))
    for doc in docs:
        if doc and doc.get('kind') in {'Deployment', 'Job'}:
            for container in doc['spec']['template']['spec']['containers']:
                if container['image'] == 'rc-bill-api:verified':
                    container['image'] = args.api
                elif container['image'] == 'rc-bill-worker:verified':
                    container['image'] = args.worker
    (out / source.name).write_text(yaml.safe_dump_all(docs, sort_keys=False), encoding='utf-8')
print('Rendered digest-pinned manifests. Existing cluster secrets/configuration are not overwritten.')
