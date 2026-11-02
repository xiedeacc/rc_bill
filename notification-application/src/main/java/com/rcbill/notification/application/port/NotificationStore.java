package com.rcbill.notification.application.port;

import com.rcbill.notification.application.NotificationUseCases;
import com.rcbill.notification.domain.NotificationView;
import java.util.Optional;
import java.util.UUID;

/** Database transaction boundary. Implementations must not perform network I/O. */
public interface NotificationStore {
    /** Lookup precedes destination revalidation/backpressure; a hit still needs fingerprint comparison. */
    Optional<IdempotencyHit> findIdempotency(
            NotificationUseCases.Caller caller, String operationScope, String idempotencyKey);

    /** Same-shard transaction writes task, payload, dedup and Outbox; mismatched fingerprint conflicts. */
    NotificationUseCases.Acceptance accept(
            NotificationUseCases.Caller caller, String idempotencyKey,
            String fingerprint, NotificationUseCases.Submission submission,
            DestinationCatalog.Snapshot destination);

    Optional<NotificationView> find(NotificationUseCases.Caller caller, UUID id);

    NotificationUseCases.AttemptPage attempts(
            NotificationUseCases.Caller caller, UUID id, int afterAttempt, int limit);

    /** Locks original-slot chain and writes child, dedup, audit and Outbox in one transaction. */
    NotificationUseCases.Acceptance replay(
            NotificationUseCases.Caller caller, UUID originalId, String idempotencyKey,
            String fingerprint, NotificationUseCases.Replay replay);

    record IdempotencyHit(String fingerprint, NotificationUseCases.Acceptance acceptance) { }
}
