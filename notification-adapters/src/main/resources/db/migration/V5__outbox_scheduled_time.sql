-- Keep the original business due time separate from the next producer retry time.
-- Nullable avoids a table rewrite; old rows use available_at until first new-code lease.
ALTER TABLE outbox_event ADD COLUMN scheduled_at timestamptz;
