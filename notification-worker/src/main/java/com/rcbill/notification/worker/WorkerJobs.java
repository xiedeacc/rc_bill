package com.rcbill.notification.worker;

import com.rcbill.notification.adapters.persistence.*;
import com.rcbill.notification.application.port.DispatchPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.*;
import java.util.concurrent.*;
import org.springframework.scheduling.annotation.Scheduled;

public final class WorkerJobs {
  private final Shards shards;
  private final PgOutboxStore outbox;
  private final PgDeliveryQueue queue;
  private final DispatchPublisher publisher;
  private final boolean relay, dispatch;
  private final MeterRegistry meters;
  private volatile Instant recoveryHeartbeat = Instant.EPOCH;

  public WorkerJobs(
      Shards shards,
      PgOutboxStore outbox,
      PgDeliveryQueue queue,
      DispatchPublisher publisher,
      String role,
      MeterRegistry meters) {
    this.shards = shards;
    this.outbox = outbox;
    this.queue = queue;
    this.publisher = publisher;
    relay = !role.equals("dispatch");
    dispatch = !role.equals("relay");
    this.meters = meters;
  }

  @Scheduled(fixedDelayString = "${notification.relay.interval-ms:100}")
  public void publishDue() {
    if (!relay) return;
    for (int shard = 0; shard < shards.all().size(); shard++) {
      try {
        var tasks =
            outbox.leaseDue(shard, 0, 100, Duration.ofSeconds(180)).stream()
                .map(
                    p -> {
                      try {
                        return publisher
                            .publish(p.event(), p.eventType())
                            .handle(
                                (ok, error) -> {
                                  if (error == null) {
                                    outbox.markPublished(p);
                                    meters.counter("notification.outbox.published").increment();
                                  } else {
                                    outbox.retryPublication(
                                        p, Instant.now().plusSeconds(5), "PUBLISH_FAILED");
                                    meters.counter("notification.outbox.failure").increment();
                                  }
                                  return (Void) null;
                                })
                            .toCompletableFuture();
                      } catch (RuntimeException e) {
                        outbox.retryPublication(p, Instant.now().plusSeconds(5), "PUBLISH_FAILED");
                        return CompletableFuture.<Void>completedFuture(null);
                      }
                    })
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(tasks).get(125, TimeUnit.SECONDS);
      } catch (Exception failure) {
        meters.counter("notification.relay.failure").increment();
      }
    }
  }

  @Scheduled(fixedDelayString = "${notification.recovery.interval-ms:1000}")
  public void recover() {
    try {
      if (dispatch) {
        queue.recoverExpiredLeases(100);
        queue.expireQueued(100);
      }
      if (relay)
        for (int shard = 0; shard < shards.all().size(); shard++)
          try {
            outbox.recoverExpiredPublicationLeases(shard, 100);
          } catch (RuntimeException failure) {
            meters.counter("notification.recovery.failure").increment();
          }
      recoveryHeartbeat = Instant.now();
    } catch (RuntimeException failure) {
      meters.counter("notification.recovery.failure").increment();
    }
  }

  @Scheduled(fixedDelayString = "${notification.cleanup.interval-ms:60000}")
  public void cleanup() {
    if (!relay) return;
    try {
      new RetentionService(shards).purge(1000);
    } catch (RuntimeException failure) {
      meters.counter("notification.cleanup.failure").increment();
    }
  }

  public boolean recovering() {
    return recoveryHeartbeat.isAfter(Instant.now().minusSeconds(15));
  }
}
