package com.rcbill.notification.adapters.persistence;

import static org.assertj.core.api.Assertions.*;

import com.rcbill.notification.adapters.config.*;
import com.rcbill.notification.adapters.security.PayloadCipher;
import com.rcbill.notification.application.*;
import com.rcbill.notification.application.port.*;
import com.rcbill.notification.domain.*;
import java.net.URI;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import tools.jackson.databind.json.JsonMapper;

/** Actual PostgreSQL transactions; no H2 and no database mocks. Uses an isolated random schema. */
class LedgerIT {
  static String base, user, password, schema;
  static Shards shards;
  static PgNotificationStore store;
  static PgDeliveryQueue queue;
  static PgOutboxStore outbox;
  static NotificationService service;
  static final NotificationUseCases.Caller caller =
      new NotificationUseCases.Caller("tenant-a", "client-a", "operator-a");

  @BeforeAll
  static void connect() throws Exception {
    base = System.getenv("TEST_PG_URL");
    assertThat(base)
        .as("TEST_PG_URL is required; integration tests must not silently skip")
        .isNotBlank();
    user = System.getenv().getOrDefault("TEST_PG_USER", "notification");
    password = System.getenv().getOrDefault("TEST_PG_PASSWORD", "notification_test");
    schema = "it_" + UUID.randomUUID().toString().replace("-", "");
    try (var c = DriverManager.getConnection(base, user, password);
        var s = c.createStatement()) {
      s.execute("CREATE SCHEMA " + schema);
    }
    String url = base + (base.contains("?") ? "&" : "?") + "currentSchema=" + schema;
    shards = new Shards(List.of(url), user, password, true, 16);
    var json = JsonMapper.builder().build();
    var cipher = new PayloadCipher(Base64.getEncoder().encodeToString(new byte[32]));
    store = new PgNotificationStore(shards, cipher, json);
    queue = new PgDeliveryQueue(shards, cipher, json);
    outbox = new PgOutboxStore(shards);
    var d =
        new Destination(
            "crm-test",
            1,
            Set.of("tenant-a"),
            Set.of("client-a"),
            true,
            URI.create("https://crm.example.com/notify"),
            "POST",
            Set.of("text/plain"),
            Set.of(),
            Map.of(),
            null,
            "Idempotency-Key",
            new DestinationCatalog.DurationPolicy(10, 20, 2592000));
    service = new NotificationService(store, new ConfiguredCatalog(List.of(d)));
  }

  @AfterAll
  static void cleanup() throws Exception {
    if (shards != null) shards.close();
    if (schema != null)
      try (var c = DriverManager.getConnection(base, user, password);
          var s = c.createStatement()) {
        s.execute("DROP SCHEMA " + schema + " CASCADE");
      }
  }

  @BeforeEach
  void reset() throws Exception {
    sql("TRUNCATE notification,delivery_chain,replay_audit CASCADE");
  }

  static void sql(String text, Object... args) throws Exception {
    try (var c = DriverManager.getConnection(base, user, password);
        var s = c.createStatement()) {
      s.execute("SET search_path TO " + schema);
      try (var p = c.prepareStatement(text)) {
        for (int i = 0; i < args.length; i++) p.setObject(i + 1, args[i]);
        p.execute();
      }
    }
  }

  static long count(String table) throws Exception {
    try (var c = DriverManager.getConnection(base, user, password);
        var s = c.createStatement();
        var r = s.executeQuery("SELECT count(*) FROM " + schema + "." + table)) {
      r.next();
      return r.getLong(1);
    }
  }

  SubmissionRequest request(String body) {
    return new SubmissionRequest("crm-test", 1, "text/plain", body, Map.of(), 60);
  }

  DispatchEvent event() {
    return outbox.leaseDue(0, 0, 100, Duration.ofSeconds(180)).stream()
        .filter(p -> p.eventType().equals("DISPATCH"))
        .findFirst()
        .orElseThrow()
        .event();
  }

  @Test
  void concurrentSameKeyCreatesExactlyOneTaskAndOutbox() throws Exception {
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var futures = new ArrayList<Future<NotificationUseCases.Acceptance>>();
      for (int i = 0; i < 32; i++)
        futures.add(
            executor.submit(() -> service.submit(caller, "same-event-key", request("original"))));
      var ids = new HashSet<UUID>();
      for (var f : futures) ids.add(f.get(30, TimeUnit.SECONDS).notificationId());
      assertThat(ids).hasSize(1);
    }
    assertThat(count("notification")).isEqualTo(1);
    assertThat(count("payload")).isEqualTo(1);
    assertThat(count("outbox_event")).isEqualTo(1);
    assertThatThrownBy(() -> service.submit(caller, "same-event-key", request("changed")))
        .isInstanceOfSatisfying(ServiceFailure.class, e -> assertThat(e.status()).isEqualTo(409));
    assertThat(count("notification")).isEqualTo(1);
  }

  @Test
  void durableHandoffCrashRecoversAndFencesLateWorker() throws Exception {
    var accepted = service.submit(caller, "crash-event-key", request("body"));
    var first = event();
    var old = queue.claim(first, Duration.ofSeconds(60)).lease();
    assertThat(queue.claim(first, Duration.ofSeconds(60)).disposition())
        .isEqualTo(DeliveryQueue.Disposition.ALREADY_DURABLE);
    // Fault injection: offset could already be committed, but HTTP was never started.
    sql(
        "UPDATE notification SET lease_until=clock_timestamp()-interval '1 second' WHERE id=?",
        accepted.notificationId());
    assertThat(queue.recoverExpiredLeases(100)).isEqualTo(1);
    assertThat(
            queue.complete(
                old,
                new DeliveryQueue.Completion(
                    NotificationStatus.SUCCEEDED,
                    AttemptOutcome.SUCCEEDED,
                    null,
                    200,
                    null,
                    1,
                    null)))
        .isFalse();
    sql(
        "UPDATE notification SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=?",
        accepted.notificationId());
    sql(
        "UPDATE outbox_event SET available_at=clock_timestamp()-interval '1 second' WHERE"
            + " notification_id=? AND schedule_epoch=1",
        accepted.notificationId());
    var next = event();
    assertThat(next.scheduleEpoch()).isEqualTo(1);
    var lease = queue.claim(next, Duration.ofSeconds(60)).lease();
    assertThat(lease.deliveryKey()).isEqualTo(old.deliveryKey());
    assertThat(
            queue.complete(
                lease,
                new DeliveryQueue.Completion(
                    NotificationStatus.SUCCEEDED,
                    AttemptOutcome.SUCCEEDED,
                    null,
                    204,
                    null,
                    1,
                    null)))
        .isTrue();
    assertThat(service.get(caller, accepted.notificationId()).orElseThrow().status())
        .isEqualTo(NotificationStatus.SUCCEEDED);
    assertThat(service.attempts(caller, accepted.notificationId(), 0, 20).items())
        .extracting(AttemptView::outcome)
        .containsExactly(AttemptOutcome.UNKNOWN, AttemptOutcome.SUCCEEDED);
  }

  @Test
  void replayIsAuditedDeduplicatedAndPreservesDeliveryIdentity() throws Exception {
    UUID original = service.submit(caller, "dead-event-key", request("body")).notificationId();
    var lease = queue.claim(event(), Duration.ofSeconds(60)).lease();
    queue.complete(
        lease,
        new DeliveryQueue.Completion(
            NotificationStatus.DEAD, AttemptOutcome.PERMANENT, null, 400, "HTTP_400", 1, null));
    var replay = new NotificationUseCases.Replay("configuration repaired", false);
    var child = service.replay(caller, original, "replay-event-key", replay);
    assertThat(service.replay(caller, original, "replay-event-key", replay).notificationId())
        .isEqualTo(child.notificationId());
    assertThatThrownBy(() -> service.replay(caller, original, "another-replay-key", replay))
        .isInstanceOf(ServiceFailure.class);
    assertThat(count("replay_audit")).isEqualTo(1);
    var childLease = queue.claim(event(), Duration.ofSeconds(60)).lease();
    assertThat(childLease.deliveryKey()).isEqualTo(lease.deliveryKey());
    assertThat(queue.loadOwnedPayload(childLease).orElseThrow().body())
        .isEqualTo("body".getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  @Test
  void readsAndReplaysAreIsolatedByTenantAndClient() {
    UUID id = service.submit(caller, "isolation-event-key", request("body")).notificationId();
    var other = new NotificationUseCases.Caller("tenant-a", "other-client", "s");
    assertThat(service.get(other, id)).isEmpty();
    assertThatThrownBy(() -> service.attempts(other, id, 0, 20))
        .isInstanceOfSatisfying(ServiceFailure.class, e -> assertThat(e.status()).isEqualTo(404));
  }

  @Test
  void repeatedBoundedPayloadCleanupMustMakeProgress() throws Exception {
    for (int i = 0; i < 2; i++) {
      UUID task =
          service.submit(caller, "cleanup-key-" + i, request("retained-body")).notificationId();
      var lease = queue.claim(event(), Duration.ofSeconds(60)).lease();
      queue.complete(
          lease,
          new DeliveryQueue.Completion(
              NotificationStatus.SUCCEEDED, AttemptOutcome.SUCCEEDED, null, 204, null, 1, null));
      sql(
          "UPDATE notification SET completed_at=clock_timestamp()-interval '8 days' WHERE id=?",
          task);
    }
    var mapper = shards.all().getFirst().mapper();
    assertThat(mapper.purgePayload(1)).isEqualTo(1);
    assertThat(mapper.purgePayload(1))
        .as("Second batch must not keep selecting the already-deleted first payload")
        .isEqualTo(1);
    assertThat(count("payload")).isZero();
  }

  @Test
  void retentionPreservesUnpublishedResponsibilitiesThenExpiresWholeChain() throws Exception {
    UUID task = service.submit(caller, "retention-event-key", request("body")).notificationId();
    var lease = queue.claim(event(), Duration.ofSeconds(60)).lease();
    queue.complete(
        lease,
        new DeliveryQueue.Completion(
            NotificationStatus.DEAD, AttemptOutcome.PERMANENT, null, 400, "HTTP_400", 1, null));
    sql(
        "UPDATE notification SET completed_at=clock_timestamp()-interval '31 days' WHERE id=?",
        task);
    sql(
        "UPDATE idempotency_record SET retain_until=clock_timestamp()-interval '1 day' WHERE"
            + " notification_id=?",
        task);
    var retention = new RetentionService(shards);
    assertThat(retention.purge(10)).isZero();
    assertThat(service.get(caller, task)).isPresent();
    sql(
        "UPDATE outbox_event SET"
            + " publish_state='PUBLISHED',published_at=clock_timestamp(),lease_token=NULL,lease_until=NULL"
            + " WHERE notification_id=?",
        task);
    assertThat(retention.purge(10)).isEqualTo(1);
    assertThat(service.get(caller, task)).isEmpty();
    assertThat(count("idempotency_record")).isZero();
    assertThat(count("delivery_chain")).isZero();
    assertThat(service.submit(caller, "retention-event-key", request("new-body")).notificationId())
        .isNotEqualTo(task);
  }

  @Test
  void anotherProcessCannotRedefineAnExistingDestinationVersion() {
    service.submit(caller, "original-version-key", request("body"));
    var changed =
        new Destination(
            "crm-test",
            1,
            Set.of("tenant-a"),
            Set.of("client-a"),
            true,
            URI.create("https://different.example.com/notify"),
            "POST",
            Set.of("text/plain"),
            Set.of(),
            Map.of(),
            null,
            "Idempotency-Key",
            new DestinationCatalog.DurationPolicy(10, 20, 2592000));
    var restarted =
        new PgNotificationStore(
            shards,
            new PayloadCipher(Base64.getEncoder().encodeToString(new byte[32])),
            JsonMapper.builder().build());
    var other = new NotificationService(restarted, new ConfiguredCatalog(List.of(changed)));
    assertThatThrownBy(() -> other.submit(caller, "new-version-request", request("body")))
        .isInstanceOfSatisfying(
            ServiceFailure.class,
            e -> assertThat(e.code()).isEqualTo("DESTINATION_VERSION_CHANGED"));
  }

  @Test
  void intentionalFutureRetryIsNotKafkaPublicationBackpressure() throws Exception {
    var longTtl = new SubmissionRequest("crm-test", 1, "text/plain", "body", Map.of(), 86400);
    UUID task = service.submit(caller, "long-delay-key", longTtl).notificationId();
    var lease = queue.claim(event(), Duration.ofSeconds(60)).lease();
    queue.complete(
        lease,
        new DeliveryQueue.Completion(
            NotificationStatus.RETRY_WAIT,
            AttemptOutcome.RETRYABLE,
            Instant.now().plusSeconds(3600),
            429,
            "HTTP_429",
            1,
            null));
    sql(
        "UPDATE outbox_event SET created_at=clock_timestamp()-interval '10 minutes' WHERE"
            + " notification_id=? AND schedule_epoch=1",
        task);
    assertThatCode(() -> service.submit(caller, "unrelated-new-key", request("another")))
        .doesNotThrowAnyException();
    sql(
        "UPDATE outbox_event SET scheduled_at=clock_timestamp()-interval '10"
            + " minutes',available_at=clock_timestamp()+interval '5 seconds' WHERE"
            + " notification_id=? AND schedule_epoch=1",
        task);
    assertThatThrownBy(() -> service.submit(caller, "backpressure-key", request("another")))
        .isInstanceOfSatisfying(
            ServiceFailure.class, e -> assertThat(e.code()).isEqualTo("OUTBOX_BACKPRESSURE"));
    assertThat(service.submit(caller, "long-delay-key", longTtl).notificationId()).isEqualTo(task);
  }

  @Test
  void totalPendingCapacityRejectsNewWorkButNotAnExistingReceipt() {
    var limitedStore =
        new PgNotificationStore(
            shards,
            new PayloadCipher(Base64.getEncoder().encodeToString(new byte[32])),
            JsonMapper.builder().build(),
            1,
            Duration.ZERO);
    var d =
        new Destination(
            "crm-test",
            1,
            Set.of("tenant-a"),
            Set.of("client-a"),
            true,
            URI.create("https://crm.example.com/notify"),
            "POST",
            Set.of("text/plain"),
            Set.of(),
            Map.of(),
            null,
            "Idempotency-Key",
            new DestinationCatalog.DurationPolicy(10, 20, 2592000));
    var limited = new NotificationService(limitedStore, new ConfiguredCatalog(List.of(d)));
    var first = limited.submit(caller, "capacity-first-key", request("body"));
    assertThatThrownBy(() -> limited.submit(caller, "capacity-second-key", request("body")))
        .isInstanceOfSatisfying(
            ServiceFailure.class, e -> assertThat(e.code()).isEqualTo("BACKLOG_CAPACITY"));
    assertThat(limited.submit(caller, "capacity-first-key", request("body")).notificationId())
        .isEqualTo(first.notificationId());
  }
}
