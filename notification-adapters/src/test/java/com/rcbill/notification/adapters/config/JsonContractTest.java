package com.rcbill.notification.adapters.config;

import static org.assertj.core.api.Assertions.*;

import com.rcbill.notification.domain.*;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class JsonContractTest {
  @Test
  void optionalFieldsAreAbsentAndTimesUseRfc3339() {
    var json = new PersistenceConfiguration().ledgerJson();
    var value =
        new NotificationView(
            UUID.randomUUID(),
            "crm-test",
            1,
            NotificationStatus.PENDING,
            0,
            Instant.EPOCH,
            Instant.EPOCH.plusSeconds(60),
            null,
            Instant.EPOCH,
            null,
            null);
    var tree = json.readTree(json.writeValueAsString(value));
    assertThat(tree.has("completedAt")).isFalse();
    assertThat(tree.has("lastErrorCode")).isFalse();
    assertThat(tree.get("acceptedAt").stringValue()).isEqualTo("1970-01-01T00:00:00Z");
  }
}
