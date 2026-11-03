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
           'image_outcome': os.environ.get('IMAGE_OUTCOME'), 'container_outcome': os.environ.get('CONTAINER_OUTCOME'),
           'runtime_outcome': os.environ.get('RUNTIME_OUTCOME'),
           'performance_outcome': os.environ.get('PERFORMANCE_OUTCOME'),
           'unit': {'tests': 0, 'failures': 0, 'errors': 0, 'skipped': 0},
           'integration': {'tests': 0, 'failures': 0, 'errors': 0, 'skipped': 0}, 'suites': [], 'report_errors': []}
for folder, kind in [('surefire-reports', 'unit'), ('failsafe-reports', 'integration')]:
    for file in Path('.').glob(f'notification-*/target/{folder}/TEST-*.xml'):
        try:
            suite = ET.parse(file).getroot()
        except ET.ParseError:
            summary['report_errors'].append(str(file))
            continue
        stats = {key: int(suite.attrib.get(key, '0')) for key in ['tests', 'failures', 'errors', 'skipped']}
        for key, value in stats.items():
            summary[kind][key] += value
        summary['suites'].append({'name': suite.attrib.get('name'), 'kind': kind, **stats})
        shutil.copy2(file, out / f'{file.parts[0]}-{file.name}')
for file in [Path('ci-maven.log'), Path('ci-services.log'), Path('ci-images.log'), Path('ci-image-smoke.log'), Path('ci-image-api.log'), Path('ci-image-worker.log'), Path('notification-worker/target/pipeline-api.log'), Path('notification-worker/target/MigrationApplication.log'), Path('notification-worker/target/BootstrapApplication.log')]:
    if file.exists():
        shutil.copy2(file, out / file.name)
for file in [*Path('.').glob('ci-demo-*'), *Path('.').glob('ci-paired-*')]:
    if file.is_file() and file.suffix != '.jfr':
        shutil.copy2(file, out / file.name)
summary['test_evidence_complete'] = (summary['tests_outcome'] == 'success' and not summary['report_errors']
                                   and all(summary[kind]['tests'] > 0 and sum(summary[kind][key] for key in ['failures', 'errors', 'skipped']) == 0
                                           for kind in ['unit', 'integration']))
(out / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(json.dumps(summary, indent=2))
if summary['tests_outcome'] == 'success' and not summary['test_evidence_complete']:
    raise SystemExit('Build reported success without complete passing test evidence.')
