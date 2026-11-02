package com.rcbill.notification.application.port;

import java.time.Instant;
import java.util.Optional;

/** Shared provider-account rate/concurrency budget, not a per-JVM-only limiter. */
public interface DispatchAdmission {
  Optional<Permit> tryAcquire(String providerBudgetKey);

  interface Permit extends AutoCloseable {
    Instant validUntil();

    /** Releases concurrency, but never refunds an already consumed rate token. */
    @Override
    void close();
  }
}
