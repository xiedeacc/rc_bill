package com.rcbill.notification.adapters.persistence;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.rcbill.notification.adapters.config.Destination;
import com.rcbill.notification.adapters.security.PayloadCipher;
import com.rcbill.notification.application.*;
import com.rcbill.notification.application.port.DestinationCatalog;
import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class AcceptanceRoundTripsTest {
  @Test
  @SuppressWarnings("unchecked")
  void acceptanceKeepsChecksButBundlesWritesAndTimestampReadback() {
    var now = Instant.parse("2026-11-03T00:00:00Z");
    var mapper =
        mock(
            LedgerMapper.class,
            invocation -> {
              if (invocation.getMethod().getName().equals("acceptBundle"))
                return Db.map("created_at", now, "dedup_count", 1L);
              return RETURNS_DEFAULTS.answer(invocation);
            });
    var shard = mock(Shards.Shard.class);
    var shards = mock(Shards.class);
    when(shards.all()).thenReturn(List.of(shard));
    when(shards.slot(anyInt())).thenReturn(shard);
    when(shard.mapper()).thenReturn(mapper);
    when(shard.transaction(any()))
        .thenAnswer(i -> ((Function<LedgerMapper, ?>) i.getArgument(0)).apply(mapper));
    var destination =
        new Destination(
            "crm-test",
            1,
            Set.of("tenant"),
            Set.of("client"),
            true,
            URI.create("https://example.com/notify"),
            "POST",
            Set.of("text/plain"),
            Set.of(),
            Map.of(),
            null,
            "Idempotency-Key",
            new DestinationCatalog.DurationPolicy(10, 20, 2592000));
    var cipher = new PayloadCipher(Base64.getEncoder().encodeToString(new byte[32]));
    var store = new PgNotificationStore(shards, cipher, JsonMapper.builder().build());
    when(mapper.destinationFingerprint(any())).thenReturn(DestinationFingerprint.of(destination));
    store.registerDestination(destination);
    clearInvocations(mapper);
    when(mapper.now()).thenReturn(now);
    when(mapper.task(any())).thenReturn(Db.map("created_at", now));
    when(mapper.insertDedup(any())).thenReturn(1);
    var accepted =
        store.accept(
            new NotificationUseCases.Caller("tenant", "client", "operator"),
            "test-event-key",
            "fingerprint",
            new SubmissionRequest("crm-test", 1, "text/plain", "body", Map.of(), 60),
            destination);
    assertThat(accepted.acceptedAt()).isEqualTo(now);
    assertThat(accepted.replayed()).isFalse();
    var calls =
        mockingDetails(mapper).getInvocations().stream().map(i -> i.getMethod().getName()).toList();
    assertThat(calls).containsExactly("boundedPending", "now", "oldestUnpublished", "acceptBundle");
  }
}
