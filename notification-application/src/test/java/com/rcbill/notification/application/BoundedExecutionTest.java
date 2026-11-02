package com.rcbill.notification.application;

import static org.assertj.core.api.Assertions.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class BoundedExecutionTest {
  @Test
  void reservesCapacityBeforeWorkExistsAndCloseIsIdempotent() {
    try (var e = new BoundedExecution(1)) {
      var first = e.reserve().orElseThrow();
      assertThat(e.reserve()).isEmpty();
      assertThat(e.inFlight()).isEqualTo(1);
      first.close();
      first.close();
      assertThat(e.inFlight()).isZero();
      try (var next = e.reserve().orElseThrow()) {
        assertThat(e.inFlight()).isEqualTo(1);
      }
    }
  }

  @Test
  void runningWorkRetainsCapacityAndUsesVirtualThread() throws Exception {
    var release = new CountDownLatch(1);
    var entered = new CountDownLatch(1);
    var finished = new CountDownLatch(1);
    var virtual = new AtomicBoolean();
    try (var e = new BoundedExecution(1)) {
      var slot = e.reserve().orElseThrow();
      slot.execute(
          () -> {
            virtual.set(Thread.currentThread().isVirtual());
            entered.countDown();
            try {
              release.await();
            } catch (InterruptedException x) {
              Thread.currentThread().interrupt();
            } finally {
              finished.countDown();
            }
          });
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
      slot.close();
      assertThat(e.reserve()).isEmpty();
      assertThat(virtual).isTrue();
      release.countDown();
      assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
    } finally {
      release.countDown();
    }
  }

  @Test
  void closedExecutorCannotLeakReservation() {
    var e = new BoundedExecution(1);
    var slot = e.reserve().orElseThrow();
    e.close();
    assertThatThrownBy(() -> slot.execute(() -> {})).isInstanceOf(RejectedExecutionException.class);
    assertThat(e.inFlight()).isZero();
    assertThat(e.reserve()).isEmpty();
  }
}
