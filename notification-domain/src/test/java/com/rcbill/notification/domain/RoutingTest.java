package com.rcbill.notification.domain;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RoutingTest {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 63, 2047, 4095})
  void uuidPreservesLogicalSlot(int slot) {
    var id = Routing.newId(slot);
    assertThat(id.version()).isEqualTo(8);
    assertThat(id.variant()).isEqualTo(2);
    assertThat(Routing.slot(id)).isEqualTo(slot);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 4096})
  void rejectsImpossibleSlots(int slot) {
    assertThatIllegalArgumentException().isThrownBy(() -> Routing.newId(slot));
  }

  @Test
  void deterministicIdentityUsesLengthFraming() {
    assertThat(Routing.slot("a", "b", "submit", "event-1"))
        .isEqualTo(Routing.slot("a", "b", "submit", "event-1"));
    var slots =
        java.util.stream.IntStream.range(0, 100)
            .map(i -> Routing.slot("tenant", "client", "submit", "event-" + i))
            .distinct()
            .count();
    assertThat(slots).isGreaterThan(90);
    assertThat(Routing.slot("a|b", "c", "submit", "k"))
        .isNotEqualTo(Routing.slot("a", "b|c", "submit", "k"));
  }

  @Test
  void recentIdsRemainTimeLocalWithinEachSlot() {
    var first = Routing.newId(4095, java.time.Instant.ofEpochMilli(4095));
    var next = Routing.newId(4095, java.time.Instant.ofEpochMilli(4096));
    assertThat(first.compareTo(next)).isLessThan(0);
    assertThat(Routing.slot(next)).isEqualTo(4095);
    assertThatIllegalArgumentException()
        .isThrownBy(() -> Routing.newId(0, java.time.Instant.ofEpochMilli(-1)));
  }
}
