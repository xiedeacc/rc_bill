from pathlib import Path
import json
import re
import yaml
from openapi_spec_validator import validate
from jsonschema import Draft202012Validator, FormatChecker

validate(yaml.safe_load(Path('contracts/openapi.yaml').read_text(encoding='utf-8')))
schema = json.loads(Path('contracts/dispatch-event.schema.json').read_text(encoding='utf-8'))
Draft202012Validator.check_schema(schema)
validator = Draft202012Validator(schema, format_checker=FormatChecker())
event = dict(schemaVersion=1, eventId='018f7fa4-3b61-7498-bf9e-c2b34bfdbf5d',
             notificationId='018f7fa4-3b61-8498-bf9e-c2b34bfdbf5d', logicalSlot=0,
             scheduleEpoch=0, destinationId='crm-test', destinationVersion=1,
             createdAt='2026-11-02T10:00:00Z', expiresAt='2026-11-03T10:00:00Z')
validator.validate(event)
for key, value in [('logicalSlot', 4096), ('scheduleEpoch', -1), ('destinationVersion', 0), ('body', 'forbidden')]:
    bad = dict(event, **{key: value})
    assert list(validator.iter_errors(bad)), (key, value)
for path in [Path('README.md'), *Path('docs').glob('*.md')]:
    for link in re.findall(r'\]\(([^)]+)\)', path.read_text(encoding='utf-8')):
        if '://' not in link and not link.startswith('#'):
            assert (path.parent / link.split('#')[0]).exists(), (path, link)
print('HTTP/event contracts, negative schema cases and document links passed.')
