-- New deployments initialize this explicitly. Upgrading a prototype with an existing
-- topology_guard row requires an operator to set its original zero-based shard position.
-- Deliberately do not guess an existing database's identity from a newly ordered URL list.
ALTER TABLE topology_guard ADD COLUMN physical_shard_id integer;
