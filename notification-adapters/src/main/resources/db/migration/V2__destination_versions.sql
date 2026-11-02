-- Shard zero acts as the low-traffic immutable configuration authority.
CREATE TABLE destination_version (
 destination_id text NOT NULL, version integer NOT NULL,
 fingerprint text NOT NULL, registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(destination_id,version)
);
