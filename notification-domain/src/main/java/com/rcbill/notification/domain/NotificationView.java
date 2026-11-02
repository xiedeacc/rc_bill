package com.rcbill.notification.domain;

import java.time.Instant;
import java.util.UUID;

/** Read model without payload, credentials or target URL. Optional fields may be null. */
public record NotificationView(
        UUID notificationId, String destinationId, int destinationVersion,
        NotificationStatus status, int attemptCount, Instant acceptedAt,
        Instant expiresAt, Instant completedAt, Instant nextAttemptAt,
        String lastErrorCode, UUID replayOf) { }
