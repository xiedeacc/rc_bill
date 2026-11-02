package com.rcbill.notification.application;

import com.rcbill.notification.domain.AttemptView;
import com.rcbill.notification.domain.NotificationView;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Target business boundary. No implementation is registered in scaffold mode. */
public interface NotificationUseCases {
  Acceptance submit(Caller caller, String idempotencyKey, Submission submission);

  Optional<NotificationView> get(Caller caller, UUID notificationId);

  AttemptPage attempts(Caller caller, UUID notificationId, int afterAttempt, int limit);

  Acceptance replay(Caller caller, UUID originalId, String idempotencyKey, Replay replay);

  /** Construct only from authenticated claims, never from request JSON. */
  record Caller(String tenantId, String clientId, String subject) {}

  /** Implementations must snapshot input and never log payloads or dynamic header values. */
  interface Submission {
    String destinationId();

    int destinationVersion();

    String contentType();

    String body();

    Map<String, String> headers();

    int ttlSeconds();
  }

  record Acceptance(UUID notificationId, Instant acceptedAt, boolean replayed) {}

  record Replay(String reason, boolean acknowledgeDuplicateRisk) {}

  record AttemptPage(List<AttemptView> items, Integer nextAfterAttempt) {}
}
