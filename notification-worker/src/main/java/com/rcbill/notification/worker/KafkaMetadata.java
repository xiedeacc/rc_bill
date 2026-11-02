package com.rcbill.notification.worker;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.LongSupplier;

public final class KafkaMetadata {
  private KafkaMetadata() {}

  static <T> T await(Callable<T> query, Duration timeout, LongSupplier nanos, Runnable pause)
      throws Exception {
    long started = nanos.getAsLong();
    Exception last = null;
    while (nanos.getAsLong() - started < timeout.toNanos()) {
      try {
        return query.call();
      } catch (Exception failure) {
        Throwable cause =
            failure instanceof java.util.concurrent.ExecutionException
                ? failure.getCause()
                : failure;
        if (!(cause instanceof org.apache.kafka.common.errors.RetriableException)
            && !(cause instanceof java.util.concurrent.TimeoutException)) throw failure;
        last = failure;
        pause.run();
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException("Metadata wait interrupted");
      }
    }
    var expired = new java.util.concurrent.TimeoutException("Kafka metadata did not become ready");
    if (last != null) expired.initCause(last);
    throw expired;
  }
}
