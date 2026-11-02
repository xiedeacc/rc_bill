package com.rcbill.notification.application.port;

import com.rcbill.notification.domain.DispatchEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Short MyBatis/PG transaction boundary for Relay; no Kafka I/O while holding database locks. */
public interface OutboxStore {
    List<Publication> leaseDue(int physicalShard, int lane, int limit, Duration leaseDuration);

    /** Called only after the Kafka send future succeeds; false means the publish lease was lost. */
    boolean markPublished(Publication publication);

    /** Releases a failed publication with backoff, preserving eventId and scheduleEpoch. */
    boolean retryPublication(Publication publication, Instant availableAt, String errorCode);

    int recoverExpiredPublicationLeases(int physicalShard, int limit);

    record Publication(DispatchEvent event, String eventType, UUID leaseToken, Instant leaseUntil) { }
}
