package com.rcbill.notification.domain;

import java.time.Instant;

/** Sanitized attempt metadata. Optional fields may be null for an unfinished attempt. */
public record AttemptView(
        int attemptNo, Instant startedAt, Instant finishedAt, AttemptOutcome outcome,
        Integer httpStatus, String errorCode, Long elapsedMs, String providerRequestId) { }
