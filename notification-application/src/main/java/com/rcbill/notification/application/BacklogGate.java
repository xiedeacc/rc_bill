package com.rcbill.notification.application;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

public final class BacklogGate {
  private record Sample(long at, String rejection) {}

  private static final class ShardSample {
    final ReentrantLock lock = new ReentrantLock();
    volatile Sample value;
  }

  private final ConcurrentHashMap<Integer, ShardSample> samples = new ConcurrentHashMap<>();
  private final long limit, ttl;
  private final LongSupplier nanos;

  public BacklogGate(long limit, Duration ttl, LongSupplier nanos) {
    if (limit < 1 || ttl.isNegative()) throw new IllegalArgumentException("capacity configuration");
    this.limit = limit;
    this.ttl = ttl.toNanos();
    this.nanos = nanos;
  }

  public void check(int shard, LongSupplier count) {
    var state = samples.computeIfAbsent(shard, ignored -> new ShardSample());
    var sample = state.value;
    if (sample == null || nanos.getAsLong() - sample.at() >= ttl) {
      // Never run JDBC inside ConcurrentHashMap.compute's monitor on Java 21 virtual threads.
      state.lock.lock();
      try {
        sample = state.value;
        long started = nanos.getAsLong();
        if (sample == null || started - sample.at() >= ttl) {
          try {
            boolean full = count.getAsLong() >= limit;
            long finished = nanos.getAsLong();
            String rejection =
                ttl > 0 && finished - started > ttl
                    ? "CAPACITY_PROBE_SLOW"
                    : full ? "BACKLOG_CAPACITY" : null;
            sample = new Sample(finished, rejection);
            state.value = sample;
          } catch (RuntimeException failure) {
            state.value = new Sample(nanos.getAsLong(), "CAPACITY_UNAVAILABLE");
            throw failure;
          }
        }
      } finally {
        state.lock.unlock();
      }
    }
    if (sample.rejection() != null) throw new ServiceFailure(503, sample.rejection());
  }
}
