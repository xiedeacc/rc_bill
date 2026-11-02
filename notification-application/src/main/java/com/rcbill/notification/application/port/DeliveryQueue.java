package com.rcbill.notification.application.port;

import com.rcbill.notification.domain.AttemptOutcome;
import com.rcbill.notification.domain.DispatchEvent;
import com.rcbill.notification.domain.NotificationStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Kafka-triggered durable handoff; all lease comparisons use database time. */
public interface DeliveryQueue {
    /**
     * Exact task/epoch CAS, never a scan of normal pending tasks. Requires admission first.
     * Return only after a committed lease or a verified durable duplicate/defer disposition.
     * Infrastructure failures and unknown tasks must fail, never look like an ACK-safe duplicate.
     */
    Handoff claim(DispatchEvent event, Duration leaseDuration);

    /** Atomically delays this epoch and creates its next Outbox event, without an HTTP attempt. */
    Handoff defer(DispatchEvent event, Instant nextAttemptAt, String reasonCode);

    /** Returns decrypted material only while this lease is current; never log the result. */
    Optional<DispatchPayload> loadOwnedPayload(Lease lease);

    /** CAS on tenant, task, IN_FLIGHT and token; atomically writes attempt + any next Outbox. */
    boolean complete(Lease lease, Completion completion);

    /** Bounded sweep: abandoned STARTED becomes UNKNOWN and next Outbox is persisted atomically. */
    int recoverExpiredLeases(int batchSize);

    /** Sweeps expired queued tasks that ordinary claim intentionally excludes. */
    int expireQueued(int batchSize);

    enum Disposition { CLAIMED, ALREADY_DURABLE, DEFERRED }

    /** Lease is present only for CLAIMED. This is a contract, not a business implementation. */
    record Handoff(Disposition disposition, Lease lease) {
        public Handoff {if(disposition==null||((disposition==Disposition.CLAIMED)!=(lease!=null)))throw new IllegalArgumentException("Invalid durable handoff result");}
    }

    record Lease(String tenantId, UUID notificationId, int logicalSlot, long scheduleEpoch, UUID token,
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
