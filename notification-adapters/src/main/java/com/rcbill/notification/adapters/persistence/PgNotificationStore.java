package com.rcbill.notification.adapters.persistence;

import static com.rcbill.notification.adapters.persistence.Db.*;

import com.rcbill.notification.adapters.config.Destination;
import com.rcbill.notification.adapters.security.PayloadCipher;
import com.rcbill.notification.application.*;
import com.rcbill.notification.application.NotificationUseCases.*;
import com.rcbill.notification.application.port.*;
import com.rcbill.notification.domain.*;
import java.time.*;
import java.util.*;
import tools.jackson.databind.json.JsonMapper;

public final class PgNotificationStore implements NotificationStore {
  public record Body(String contentType, String body, Map<String, String> headers) {
    @Override
    public String toString() {
      return "Body[REDACTED]";
    }
  }

  private final Shards shards;
  private final PayloadCipher cipher;
  private final JsonMapper json;
  private final Map<String, String> destinationVersions =
      new java.util.concurrent.ConcurrentHashMap<>();
  private final BacklogGate backlog;
  private final int pendingLimit;

  public PgNotificationStore(Shards shards, PayloadCipher cipher, JsonMapper json) {
    this(shards, cipher, json, 50000, Duration.ofSeconds(1));
  }

  public PgNotificationStore(
      Shards shards,
      PayloadCipher cipher,
      JsonMapper json,
      int maximumPending,
      Duration samplingTtl) {
    if (maximumPending < 1) throw new IllegalArgumentException("maximumPending");
    this.shards = shards;
    this.cipher = cipher;
    this.json = json;
    pendingLimit =
        Math.max(
            1, (int) ((maximumPending + (long) shards.all().size() - 1) / shards.all().size()));
    backlog = new BacklogGate(pendingLimit, samplingTtl, System::nanoTime);
  }

  private static int slot(Caller c, String scope, String key) {
    return scope.startsWith("replay:")
        ? Routing.slot(UUID.fromString(scope.substring(7)))
        : Routing.slot(c.tenantId(), c.clientId(), scope, key);
  }

  @Override
  public Optional<IdempotencyHit> findIdempotency(Caller c, String scope, String key) {
    var row =
        shards
            .slot(slot(c, scope, key))
            .mapper()
            .dedup(map("tenant", c.tenantId(), "client", c.clientId(), "scope", scope, "key", key));
    return row == null
        ? Optional.empty()
        : Optional.of(
            new IdempotencyHit(
                (String) row.get("fingerprint"),
                new Acceptance(
                    id(row.get("notification_id")), instant(row.get("created_at")), true)));
  }

  @Override
  public Acceptance accept(
      Caller c, String key, String fingerprint, Submission s, DestinationCatalog.Snapshot d) {
    registerDestination(d);
    int slot = slot(c, "submit", key);
    var shard = shards.slot(slot);
    backlog.check(slot % shards.all().size(), () -> shard.mapper().boundedPending(pendingLimit));
    try {
      return shard.transaction(
          m -> {
            Instant now = m.now(), oldest = m.oldestUnpublished();
            if (oldest != null && oldest.plusSeconds(300).isBefore(now))
              throw new ServiceFailure(503, "OUTBOX_BACKPRESSURE");
            UUID task = Routing.newId(slot);
            var result =
                m.acceptBundle(
                    map(
                        "id",
                        task,
                        "tenant",
                        c.tenantId(),
                        "client",
                        c.clientId(),
                        "slot",
                        slot,
                        "destination",
                        s.destinationId(),
                        "version",
                        s.destinationVersion(),
                        "snapshot",
                        json.writeValueAsString(d),
                        "fingerprint",
                        fingerprint,
                        "deliveryKey",
                        UUID.randomUUID().toString(),
                        "expires",
                        now.plusSeconds(s.ttlSeconds()),
                        "cipher",
                        cipher.encrypt(
                            task,
                            json.writeValueAsString(
                                new Body(s.contentType(), s.body(), s.headers()))),
                        "key",
                        key,
                        "event",
                        UUID.randomUUID(),
                        "available",
                        now));
            if (number(result, "dedup_count") != 1) throw new Duplicate();
            return new Acceptance(task, instant(result.get("created_at")), false);
          });
    } catch (Duplicate race) {
      return NotificationService.existing(
          findIdempotency(c, "submit", key).orElseThrow(), fingerprint);
    }
  }

  public void registerDestination(DestinationCatalog.Snapshot d) {
    String key = d.id() + ":" + d.version(), fingerprint = DestinationFingerprint.of(d);
    String cached = destinationVersions.get(key);
    if (cached == null) {
      cached =
          shards
              .all()
              .getFirst()
              .transaction(
                  m -> {
                    var args =
                        map("id", d.id(), "version", d.version(), "fingerprint", fingerprint);
                    m.registerDestination(args);
                    return m.destinationFingerprint(args);
                  });
      destinationVersions.putIfAbsent(key, cached);
    }
    if (!cached.equals(fingerprint)) throw new ServiceFailure(422, "DESTINATION_VERSION_CHANGED");
  }

  private void create(
      LedgerMapper m,
      Caller c,
      UUID task,
      UUID root,
      UUID replay,
      String delivery,
      int slot,
      String snapshot,
      String fp,
      String destination,
      int version,
      Instant expires) {
    m.insertTask(
        map(
            "id",
            task,
            "tenant",
            c.tenantId(),
            "client",
            c.clientId(),
            "slot",
            slot,
            "destination",
            destination,
            "version",
            version,
            "snapshot",
            snapshot,
            "fingerprint",
            fp,
            "root",
            root,
            "replayOf",
            replay,
            "deliveryKey",
            delivery,
            "expires",
            expires));
  }

  public static void event(
      LedgerMapper m, UUID task, int slot, long epoch, String type, Instant available) {
    m.outbox(
        map(
            "event",
            UUID.randomUUID(),
            "id",
            task,
            "slot",
            slot,
            "epoch",
            epoch,
            "type",
            type,
            "available",
            available));
  }

  public static Map<String, Object> owned(Map<String, Object> row, Caller c) {
    if (row == null
        || !Objects.equals(row.get("tenant_id"), c.tenantId())
        || !Objects.equals(row.get("client_id"), c.clientId()))
      throw new ServiceFailure(404, "NOT_FOUND");
    return row;
  }

  @Override
  public Optional<NotificationView> find(Caller c, UUID task) {
    var row =
        shards
            .task(task)
            .mapper()
            .ownedTask(map("id", task, "tenant", c.tenantId(), "client", c.clientId()));
    if (row == null
        || !Objects.equals(row.get("tenant_id"), c.tenantId())
        || !Objects.equals(row.get("client_id"), c.clientId())) return Optional.empty();
    return Optional.of(view(row));
  }

  private NotificationView view(Map<String, Object> row) {
    return new NotificationView(
        id(row.get("id")),
        (String) row.get("destination_id"),
        integer(row, "destination_version"),
        NotificationStatus.valueOf((String) row.get("status")),
        integer(row, "attempt_count"),
        instant(row.get("created_at")),
        instant(row.get("expires_at")),
        instant(row.get("completed_at")),
        instant(row.get("next_attempt_at")),
        (String) row.get("last_error"),
        id(row.get("replay_of")));
  }

  @Override
  public AttemptPage attempts(Caller c, UUID task, int after, int limit) {
    var m = shards.task(task).mapper();
    owned(m.ownedTask(map("id", task, "tenant", c.tenantId(), "client", c.clientId())), c);
    var rows =
        m.attempts(
            map(
                "id",
                task,
                "tenant",
                c.tenantId(),
                "client",
                c.clientId(),
                "after",
                after,
                "limit",
                limit + 1));
    var items =
        rows.stream()
            .limit(limit)
            .map(
                r ->
                    new AttemptView(
                        integer(r, "attempt_no"),
                        instant(r.get("started_at")),
                        instant(r.get("finished_at")),
                        AttemptOutcome.valueOf((String) r.get("outcome")),
                        (Integer) r.get("http_status"),
                        (String) r.get("error_code"),
                        r.get("elapsed_ms") == null ? null : number(r, "elapsed_ms"),
                        (String) r.get("provider_request_id")))
            .toList();
    return new AttemptPage(items, rows.size() > limit ? items.getLast().attemptNo() : null);
  }

  @Override
  public Acceptance replay(Caller c, UUID original, String key, String fingerprint, Replay replay) {
    String scope = "replay:" + original;
    try {
      return shards
          .task(original)
          .transaction(
              m -> {
                var source =
                    owned(
                        m.ownedTask(
                            map("id", original, "tenant", c.tenantId(), "client", c.clientId())),
                        c);
                UUID root = id(source.get("root_id"));
                var chain = m.lockChain(root);
                if (chain == null) throw new ServiceFailure(404, "NOT_FOUND");
                var previous =
                    m.dedup(
                        map(
                            "tenant",
                            c.tenantId(),
                            "client",
                            c.clientId(),
                            "scope",
                            scope,
                            "key",
                            key));
                if (previous != null) throw new Duplicate();
                if (!Set.of("DEAD", "EXPIRED").contains(source.get("status"))
                    || chain.get("active_id") != null)
                  throw new ServiceFailure(409, "REPLAY_STATE_CONFLICT");
                byte[] payload = m.payload(original);
                if (payload == null) throw new ServiceFailure(410, "PAYLOAD_GONE");
                var d = json.readValue((String) source.get("snapshot"), Destination.class);
                Instant now = m.now();
                Instant first = instant(m.task(root).get("created_at"));
                boolean safe =
                    d.idempotencyHeader() != null
                        && d.policy().idempotencyWindowSeconds() > 0
                        && first
                            .plusSeconds(d.policy().idempotencyWindowSeconds())
                            .isAfter(now.plusSeconds(86400));
                if (!safe && !replay.acknowledgeDuplicateRisk())
                  throw new ServiceFailure(409, "DUPLICATE_RISK_ACK_REQUIRED");
                int slot = integer(source, "slot");
                UUID task = Routing.newId(slot);
                create(
                    m,
                    c,
                    task,
                    root,
                    original,
                    (String) source.get("delivery_key"),
                    slot,
                    (String) source.get("snapshot"),
                    fingerprint,
                    (String) source.get("destination_id"),
                    integer(source, "destination_version"),
                    now.plusSeconds(86400));
                m.insertPayload(
                    map(
                        "id",
                        task,
                        "cipher",
                        cipher.encrypt(task, cipher.decrypt(original, payload))));
                if (m.insertDedup(
                        map(
                            "tenant",
                            c.tenantId(),
                            "client",
                            c.clientId(),
                            "scope",
                            scope,
                            "key",
                            key,
                            "fingerprint",
                            fingerprint,
                            "id",
                            task))
                    == 0) throw new Duplicate();
                m.active(map("root", root, "active", task));
                m.audit(
                    map(
                        "auditId",
                        UUID.randomUUID(),
                        "original",
                        original,
                        "id",
                        task,
                        "actor",
                        c.subject(),
                        "reason",
                        replay.reason(),
                        "risk",
                        replay.acknowledgeDuplicateRisk()));
                event(m, task, slot, 0, "DISPATCH", now);
                return new Acceptance(task, instant(m.task(task).get("created_at")), false);
              });
    } catch (Duplicate race) {
      return NotificationService.existing(
          findIdempotency(c, scope, key).orElseThrow(), fingerprint);
    }
  }

  private static final class Duplicate extends RuntimeException {}
}
