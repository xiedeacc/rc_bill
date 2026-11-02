package com.rcbill.notification.domain;

/** UNKNOWN means an external effect cannot be ruled out. */
public enum AttemptOutcome {
    STARTED, SUCCEEDED, RETRYABLE, PERMANENT, UNKNOWN
}
