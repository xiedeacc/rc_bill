"""Generate disposable local-only fixtures. Never reuse these keys or settings in production."""
import base64
import json
import os
import re
from pathlib import Path
import time
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

folder = Path('tmp/demo').resolve()
folder.mkdir(parents=True, exist_ok=True)
private = folder / 'private.pem'
if private.exists():
    key = serialization.load_pem_private_key(private.read_bytes(), password=None)
else:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    private.chmod(0o600)
public = folder / 'public.pem'
public.write_bytes(key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo))
def encode(value):
    return base64.urlsafe_b64encode(json.dumps(value, separators=(',', ':')).encode()).rstrip(b'=')
header = encode({'alg': 'RS256', 'typ': 'JWT'})
claims = encode({'iss': 'https://demo.example', 'aud': ['notifications'], 'sub': 'demo-operator',
                 'tenant_id': 'demo-tenant', 'client_id': 'demo-client',
                 'scope': 'notifications:write notifications:read notifications:replay', 'exp': int(time.time()) + 3600})
message = header + b'.' + claims
signature = key.sign(message, padding.PKCS1v15(), hashes.SHA256())
(folder / 'token.txt').write_text((message + b'.' + base64.urlsafe_b64encode(signature).rstrip(b'=')).decode(), encoding='utf-8')
destination = {'id': 'demo-supplier', 'version': 1, 'allowedTenants': ['demo-tenant'], 'allowedClients': ['demo-client'],
               'enabled': True, 'endpoint': 'http://127.0.0.1:9909/notify', 'method': 'POST',
               'contentTypes': ['application/json', 'text/plain'], 'allowedDynamicHeaders': ['X-Event-ID'],
               'staticHeaders': {}, 'credentialRef': None, 'idempotencyHeader': 'Idempotency-Key',
               'policy': {'timeoutSeconds': 2, 'maxAttempts': 20, 'idempotencyWindowSeconds': 2592000}}
destinations = folder / 'destinations.json'
if not destinations.exists():
    destinations.write_text(json.dumps([destination], indent=2), encoding='utf-8')
settings = {'PG_URL': 'jdbc:postgresql://127.0.0.1:15432/notification', 'PG_USER': 'notification',
            'PG_PASSWORD': 'notification_test', 'DB_MIGRATE': 'false', 'KAFKA_PARTITIONS': '16',
            'KAFKA_BOOTSTRAP_SERVERS': 'localhost:19092,localhost:19093,localhost:19094',
            'NOTIFICATION_ENCRYPTION_KEY': base64.b64encode(os.urandom(32)).decode(),
            'DESTINATIONS_FILE': str(destinations), 'JWT_ISSUER': 'https://demo.example',
            'JWT_AUDIENCE': 'notifications', 'NOTIFICATION_SECURITY_PUBLIC_KEY_FILE': str(public),
            'NOTIFICATION_ALLOW_LOOPBACK': 'true', 'QUOTA_URL': 'http://127.0.0.1:9909'}
environment_file = folder / 'environment.json'
if environment_file.exists():
    settings.update(json.loads(environment_file.read_text(encoding='utf-8')))

fixture = os.environ.get('PERF_FIXTURE_ID')
if fixture:
    if not re.fullmatch(r'perf_[a-z0-9_]{1,48}', fixture):
        raise SystemExit('Invalid isolated performance fixture identifier')
    settings.update({'PG_URL': 'jdbc:postgresql://127.0.0.1:15432/notification?currentSchema=' + fixture,
                     'NOTIFICATION_KAFKA_DISPATCH_TOPIC': fixture + '.dispatch',
                     'NOTIFICATION_KAFKA_DEAD_TOPIC': fixture + '.dead',
                     'NOTIFICATION_KAFKA_QUARANTINE_TOPIC': fixture + '.quarantine',
                     'NOTIFICATION_KAFKA_GROUP': fixture + '.group'})
environment_file.write_text(json.dumps(settings, indent=2), encoding='utf-8')
environment_file.chmod(0o600)
print('Generated ignored local fixtures under tmp/demo; JWT expires in one hour.')
