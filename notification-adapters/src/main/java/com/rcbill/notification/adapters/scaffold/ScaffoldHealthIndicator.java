package com.rcbill.notification.adapters.scaffold;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

public final class ScaffoldHealthIndicator implements HealthIndicator {
    @Override
    public Health health() {
        return Health.down().withDetail("stage", "scaffold")
                .withDetail("reason", "Durable acceptance and delivery are not implemented").build();
    }
}
