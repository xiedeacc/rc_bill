CREATE INDEX CONCURRENTLY task_backlog ON notification(id) WHERE status IN ('PENDING','RETRY_WAIT','IN_FLIGHT');
