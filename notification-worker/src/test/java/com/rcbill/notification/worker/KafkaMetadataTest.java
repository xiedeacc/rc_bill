package com.rcbill.notification.worker;

import static org.assertj.core.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.common.errors.*;
import org.junit.jupiter.api.Test;

class KafkaMetadataTest {
  @Test
  void creationAcknowledgementCanPrecedeBrokerVisibility() throws Exception {
    var time = new AtomicLong();
    var calls = new AtomicInteger();
    String ready =
        KafkaMetadata.await(
            () -> {
              if (calls.incrementAndGet() < 3)
                throw new ExecutionException(new UnknownTopicOrPartitionException("propagating"));
              return "ready";
            },
            Duration.ofSeconds(1),
            time::get,
            () -> time.addAndGet(100_000_000));
    assertThat(ready).isEqualTo("ready");
    assertThat(calls.get()).isEqualTo(3);
  }

  @Test
  void permissionErrorsAreNotHiddenAsMetadataRetries() {
    var calls = new AtomicInteger();
    assertThatThrownBy(
            () ->
                KafkaMetadata.await(
                    () -> {
                      calls.incrementAndGet();
                      throw new ExecutionException(new TopicAuthorizationException("denied"));
                    },
                    Duration.ofSeconds(1),
                    () -> 0,
                    () -> {
                      throw new AssertionError("must not retry authorization");
                    }))
        .hasCauseInstanceOf(TopicAuthorizationException.class);
    assertThat(calls.get()).isEqualTo(1);
  }

  @Test
  void propagationWaitHasAnOverallDeadline() {
    var time = new AtomicLong();
    assertThatThrownBy(
            () ->
                KafkaMetadata.await(
                    () -> {
                      throw new ExecutionException(new UnknownTopicOrPartitionException("missing"));
                    },
                    Duration.ofMillis(300),
                    time::get,
                    () -> time.addAndGet(100_000_000)))
        .isInstanceOf(java.util.concurrent.TimeoutException.class);
  }
}
