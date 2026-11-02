package com.rcbill.notification.domain;

import static org.assertj.core.api.Assertions.*;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RetryPolicyTest {
  final Instant now = Instant.parse("2026-11-02T12:00:00Z");

  RetryPolicy.Decision decide(int attempt, int status, String after, String error) {
    return RetryPolicy.decide(
        now,
        now.plusSeconds(86400),
        attempt,
        20,
        status == 0 ? null : status,
        after,
        error,
        () -> 0.5);
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 201, 202, 204, 299})
  void succeedsOnHttpAcceptance(int status) {
    assertThat(decide(1, status, null, null).status()).isEqualTo(NotificationStatus.SUCCEEDED);
  }

  @ParameterizedTest
  @ValueSource(ints = {301, 302, 307, 400, 401, 403, 404, 409, 422, 501})
  void permanentStatusIsNotRetried(int status) {
    assertThat(decide(1, status, null, null).status()).isEqualTo(NotificationStatus.DEAD);
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 425, 429, 500, 502, 503, 504})
  void retryIsDurableAndBounded(int status) {
    assertThat(decide(1, status, null, null).nextAttemptAt()).isEqualTo(now.plusSeconds(1));
    assertThat(decide(20, status, null, null).status()).isEqualTo(NotificationStatus.DEAD);
  }

  @Test
  void retryAfterNeverGetsShortened() {
    assertThat(decide(1, 429, "120", null).nextAttemptAt()).isEqualTo(now.plusSeconds(120));
    assertThat(decide(1, 503, "Mon, 2 Nov 2026 12:02:00 GMT", null).nextAttemptAt())
        .isEqualTo(now.plusSeconds(120));
    assertThat(decide(1, 429, "9999999999999999999999999", null).status())
        .isEqualTo(NotificationStatus.EXPIRED);
    assertThat(decide(1, 429, "invalid", null).nextAttemptAt()).isEqualTo(now.plusSeconds(1));
  }

  @Test
  void timeoutPreservesUncertainEffect() {
    assertThat(decide(1, 0, null, "IO_UNKNOWN").outcome()).isEqualTo(AttemptOutcome.UNKNOWN);
    assertThat(decide(1, 0, null, "TLS_ERROR").status()).isEqualTo(NotificationStatus.DEAD);
  }

  @Test
  void noZeroDelaySpinOrAttemptAfterDeadline() {
    var d = RetryPolicy.decide(now, now.plusSeconds(10), 1, 20, 500, null, null, () -> 0);
    assertThat(d.nextAttemptAt()).isEqualTo(now.plusMillis(200));
    assertThat(RetryPolicy.decide(now, now, 1, 20, 500, null, null, () -> 0).status())
        .isEqualTo(NotificationStatus.EXPIRED);
  }
}
