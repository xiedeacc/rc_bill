package com.rcbill.notification.application.port;

import com.rcbill.notification.application.NotificationUseCases;
import com.rcbill.notification.domain.NotificationView;
import java.util.Optional;
import java.util.UUID;

/** Database transaction boundary. Implementations must not perform network I/O. */
public interface NotificationStore {
    /** Atomically writes task and deduplication record; a mismatched fingerprint conflicts. */
    NotificationUseCases.Acceptance accept(
            NotificationUseCases.Caller caller, String idempotencyKey,
            String fingerprint, NotificationUseCases.Submission submission,
            DestinationCatalog.Snapshot destination);

    Optional<NotificationView> find(NotificationUseCases.Caller caller, UUID id);

    NotificationUseCases.AttemptPage attempts(
            NotificationUseCases.Caller caller, UUID id, int afterAttempt, int limit);

    /** Locks the delivery chain, checks eligibility, writes child + dedup + audit together. */
    NotificationUseCases.Acceptance replay(
            NotificationUseCases.Caller caller, UUID originalId, String idempotencyKey,
            String fingerprint, NotificationUseCases.Replay replay);
}
