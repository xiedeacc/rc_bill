CREATE INDEX CONCURRENTLY outbox_original_due ON outbox_event((COALESCE(scheduled_at,available_at))) WHERE publish_state<>'PUBLISHED';
