package com.rcbill.notification.domain;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.function.DoubleSupplier;

public final class RetryPolicy {
  public record Decision(
      NotificationStatus status, AttemptOutcome outcome, Instant nextAttemptAt) {}

  public static Decision decide(
      Instant now,
      Instant expiresAt,
      int attempt,
      int maximum,
      Integer httpStatus,
      String retryAfter,
      String error,
      DoubleSupplier random) {
    if (httpStatus != null && httpStatus >= 200 && httpStatus < 300 && error == null)
      return new Decision(NotificationStatus.SUCCEEDED, AttemptOutcome.SUCCEEDED, null);
    boolean unknown = httpStatus == null && (error == null || error.equals("IO_UNKNOWN"));
    boolean retry =
        unknown
            || (error == null
                && httpStatus != null
                && Set.of(408, 425, 429, 500, 502, 503, 504).contains(httpStatus));
    AttemptOutcome outcome =
        unknown
            ? AttemptOutcome.UNKNOWN
            : retry ? AttemptOutcome.RETRYABLE : AttemptOutcome.PERMANENT;
    if (!retry) return new Decision(NotificationStatus.DEAD, outcome, null);
    if (!now.isBefore(expiresAt)) return new Decision(NotificationStatus.EXPIRED, outcome, null);
    if (attempt >= maximum) return new Decision(NotificationStatus.DEAD, outcome, null);
    long capMillis = Math.min(3_600_000L, 2000L << Math.min(30, Math.max(0, attempt - 1)));
    long jitter =
        Math.max(200, (long) (Math.max(0, Math.min(1, random.getAsDouble())) * capMillis));
    long after =
        (httpStatus != null && (httpStatus == 429 || httpStatus == 503))
            ? retryAfterMillis(retryAfter, now)
            : 0;
    long delay = Math.max(jitter, after);
    if (delay >= Duration.between(now, expiresAt).toMillis())
      return new Decision(NotificationStatus.EXPIRED, outcome, null);
    return new Decision(NotificationStatus.RETRY_WAIT, outcome, now.plusMillis(delay));
  }

  private static long retryAfterMillis(String value, Instant now) {
    if (value == null) return 0;
    value = value.trim();
    if (value.matches("[0-9]+")) {
      try {
        return Math.multiplyExact(Long.parseLong(value), 1000);
      } catch (ArithmeticException | NumberFormatException overflow) {
        return Long.MAX_VALUE;
      }
    }
    try {
      return Math.max(
          0,
          Duration.between(
                  now, ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant())
              .toMillis());
    } catch (RuntimeException invalid) {
      return 0;
    }
  }
}
