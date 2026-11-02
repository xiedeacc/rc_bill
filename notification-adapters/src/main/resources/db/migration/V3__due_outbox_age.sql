-- Planned future retries are not broker publication backlog.
CREATE INDEX CONCURRENTLY outbox_unpublished_due ON outbox_event(available_at) WHERE publish_state<>'PUBLISHED';
