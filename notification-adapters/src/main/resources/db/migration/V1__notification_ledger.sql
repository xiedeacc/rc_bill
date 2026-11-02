CREATE TABLE topology_guard (singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), shard_count integer NOT NULL, partition_count integer NOT NULL);
CREATE TABLE notification (
 id uuid PRIMARY KEY, tenant_id text NOT NULL, client_id text NOT NULL, slot integer NOT NULL CHECK(slot BETWEEN 0 AND 4095),
 destination_id text NOT NULL, destination_version integer NOT NULL, snapshot text NOT NULL,
 fingerprint text NOT NULL, root_id uuid NOT NULL, replay_of uuid, delivery_key text NOT NULL,
 status text NOT NULL CHECK(status IN ('PENDING','IN_FLIGHT','RETRY_WAIT','SUCCEEDED','DEAD','EXPIRED')),
 attempt_count integer NOT NULL DEFAULT 0 CHECK(attempt_count BETWEEN 0 AND 20), schedule_epoch bigint NOT NULL DEFAULT 0,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), expires_at timestamptz NOT NULL,
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(), completed_at timestamptz, last_error text,
 lease_token uuid, lease_until timestamptz,
 CHECK ((status='IN_FLIGHT') = (lease_token IS NOT NULL AND lease_until IS NOT NULL)),
 CHECK ((status IN ('SUCCEEDED','DEAD','EXPIRED')) = (completed_at IS NOT NULL))
);
CREATE TABLE payload (notification_id uuid PRIMARY KEY REFERENCES notification(id), ciphertext bytea NOT NULL);
CREATE TABLE idempotency_record (
 tenant_id text NOT NULL, client_id text NOT NULL, scope text NOT NULL, key text NOT NULL,
 fingerprint text NOT NULL, notification_id uuid NOT NULL REFERENCES notification(id),
 retain_until timestamptz NOT NULL DEFAULT clock_timestamp()+interval '30 days',
 PRIMARY KEY(tenant_id,client_id,scope,key)
);
CREATE TABLE delivery_chain (root_id uuid PRIMARY KEY, active_id uuid);
CREATE TABLE delivery_attempt (
 notification_id uuid NOT NULL REFERENCES notification(id), attempt_no integer NOT NULL, lease_token uuid NOT NULL,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
 outcome text NOT NULL CHECK(outcome IN ('STARTED','SUCCEEDED','RETRYABLE','PERMANENT','UNKNOWN')),
 http_status integer, error_code text, elapsed_ms bigint, provider_request_id text,
 PRIMARY KEY(notification_id,attempt_no)
);
CREATE TABLE outbox_event (
 event_id uuid PRIMARY KEY, notification_id uuid NOT NULL REFERENCES notification(id), slot integer NOT NULL,
 schedule_epoch bigint NOT NULL, event_type text NOT NULL DEFAULT 'DISPATCH',
 available_at timestamptz NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 publish_state text NOT NULL DEFAULT 'PENDING' CHECK(publish_state IN ('PENDING','IN_FLIGHT','PUBLISHED')),
 lease_token uuid, lease_until timestamptz, published_at timestamptz,
 UNIQUE(notification_id,schedule_epoch,event_type)
);
CREATE TABLE replay_audit (id uuid PRIMARY KEY, original_id uuid NOT NULL, replay_id uuid NOT NULL, actor text NOT NULL,
 reason text NOT NULL, risk_acknowledged boolean NOT NULL, created_at timestamptz NOT NULL DEFAULT clock_timestamp());
CREATE INDEX outbox_due ON outbox_event(available_at,event_id) WHERE publish_state='PENDING';
CREATE INDEX outbox_expired ON outbox_event(lease_until,event_id) WHERE publish_state='IN_FLIGHT';
CREATE INDEX outbox_age ON outbox_event(created_at) WHERE publish_state<>'PUBLISHED';
CREATE INDEX task_expired_lease ON notification(lease_until,id) WHERE status='IN_FLIGHT';
CREATE INDEX task_expired_ttl ON notification(expires_at,id) WHERE status IN ('PENDING','RETRY_WAIT');
CREATE INDEX task_completed ON notification(completed_at,id) WHERE completed_at IS NOT NULL;
CREATE INDEX dedup_expiry ON idempotency_record(retain_until);
CREATE INDEX task_root ON notification(root_id);
