"""Launch one local process from ignored demo settings without printing keys or tokens."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

if len(sys.argv) != 2 or sys.argv[1] not in {'api', 'worker', 'migrate', 'bootstrap'}:
    raise SystemExit('Usage: python scripts/run-demo.py api|worker|migrate|bootstrap')
role = sys.argv[1]
settings = json.loads(Path('tmp/demo/environment.json').read_text(encoding='utf-8'))
environment = dict(os.environ, **settings)
java = str(Path(environment['JAVA_HOME']) / 'bin' / ('java.exe' if os.name == 'nt' else 'java')) if environment.get('JAVA_HOME') else shutil.which('java')
if not java:
    raise SystemExit('Set JAVA_HOME to JDK 21 or a supported newer JDK.')
module = 'notification-api' if role == 'api' else 'notification-worker'
jar = str(Path(f'{module}/target/{module}-0.1.0-SNAPSHOT.jar').resolve())
if role == 'api' and environment.get('DEMO_API_JAR'):
    jar = str(Path(environment['DEMO_API_JAR']).resolve(strict=True))
if role in {'migrate', 'bootstrap'}:
    main = 'MigrationApplication' if role == 'migrate' else 'BootstrapApplication'
    command = [java, '-Dloader.main=com.rcbill.notification.worker.' + main, '-cp', jar, 'org.springframework.boot.loader.launch.PropertiesLauncher']
else:
    command = [java, '-jar', jar, '--spring.profiles.active=runtime']
if os.name != 'nt':
    os.execvpe(java, command, environment)
raise SystemExit(subprocess.call(command, env=environment))
