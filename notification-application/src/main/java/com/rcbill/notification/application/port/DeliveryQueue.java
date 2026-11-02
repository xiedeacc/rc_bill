package com.rcbill.notification.application.port;

import com.rcbill.notification.domain.AttemptOutcome;
import com.rcbill.notification.domain.NotificationStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Persistent scheduling boundary; all lease comparisons use database time. */
public interface DeliveryQueue {
    /** The dispatcher must already hold enough admission permits for this destination. */
    List<Lease> claim(String destinationId, int freeSlots, Duration leaseDuration);

    /** Returns decrypted material only while this lease is current; never log the result. */
    Optional<DispatchPayload> loadOwnedPayload(Lease lease);

    /** CAS on tenant, task, IN_FLIGHT and token; false means ownership was lost. */
    boolean complete(Lease lease, Completion completion);

    /** Bounded sweep: abandoned STARTED attempts become UNKNOWN before rescheduling. */
    int recoverExpiredLeases(int batchSize);

    /** Sweeps expired queued tasks that ordinary claim intentionally excludes. */
    int expireQueued(int batchSize);

    record Lease(String tenantId, UUID notificationId, UUID token,
                 int attemptNo, Instant leaseUntil, Instant expiresAt, String deliveryKey) { }

    interface DispatchPayload {
        DestinationCatalog.Snapshot destination();
        String contentType();
        byte[] body();
        Map<String, String> dynamicHeaders();
    }

    record Completion(NotificationStatus nextStatus, AttemptOutcome outcome,
                      Instant nextAttemptAt, Integer httpStatus, String errorCode,
                      long elapsedMs, String providerRequestId) { }
}
