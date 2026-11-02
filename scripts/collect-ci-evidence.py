"""Keep actual CI outcomes and JUnit results; never turn skipped/missing tests into success."""
import json
import os
from pathlib import Path
import shutil
import xml.etree.ElementTree as ET

out = Path('ci-evidence-output')
out.mkdir(exist_ok=True)
summary = {'sha': os.environ.get('GITHUB_SHA'), 'run_id': os.environ.get('GITHUB_RUN_ID'),
           'tests_outcome': os.environ.get('TEST_OUTCOME'), 'infrastructure_outcome': os.environ.get('INFRA_OUTCOME'),
           'unit': {'tests': 0, 'failures': 0, 'errors': 0, 'skipped': 0},
           'integration': {'tests': 0, 'failures': 0, 'errors': 0, 'skipped': 0}, 'suites': []}
for folder, kind in [('surefire-reports', 'unit'), ('failsafe-reports', 'integration')]:
    for file in Path('.').glob(f'notification-*/target/{folder}/TEST-*.xml'):
        suite = ET.parse(file).getroot()
        stats = {key: int(suite.attrib.get(key, '0')) for key in ['tests', 'failures', 'errors', 'skipped']}
        for key, value in stats.items():
            summary[kind][key] += value
        summary['suites'].append({'name': suite.attrib.get('name'), 'kind': kind, **stats})
        shutil.copy2(file, out / f'{file.parts[0]}-{file.name}')
for file in [Path('ci-maven.log'), Path('ci-services.log'), Path('notification-worker/target/pipeline-api.log')]:
    if file.exists():
        shutil.copy2(file, out / file.name)
(out / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(json.dumps(summary, indent=2))
