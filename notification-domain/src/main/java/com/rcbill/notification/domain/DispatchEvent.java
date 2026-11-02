package com.rcbill.notification.domain;

import java.time.Instant;
import java.util.UUID;

/** Payload-free Kafka event. Consumers validate this against the authoritative task row. */
public record DispatchEvent(
        int schemaVersion, UUID eventId, UUID notificationId, int logicalSlot,
        long scheduleEpoch, String destinationId, Instant createdAt, Instant expiresAt) { }
