package com.rcbill.notification.adapters.scaffold;

import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

import static org.assertj.core.api.Assertions.assertThat;

class ScaffoldHealthIndicatorTest {
    @Test
    void neverReportsProductionReadiness() {
        var health = new ScaffoldHealthIndicator().health();
        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("stage", "scaffold");
    }
}
