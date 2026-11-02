package com.rcbill.notification.worker;

import com.rcbill.notification.adapters.persistence.Shards;
import java.util.*;

/** Standalone migration entry point; needs only database credentials, not JWT/provider secrets. */
public final class MigrationApplication {
  public static void main(String[] args) {
    var env = System.getenv();
    try (var ignored =
        new Shards(
            Arrays.asList(Objects.requireNonNull(env.get("PG_URL"), "PG_URL").split(",")),
            Objects.requireNonNull(env.get("PG_USER"), "PG_USER"),
            Objects.requireNonNull(env.get("PG_PASSWORD"), "PG_PASSWORD"),
            true,
            Integer.parseInt(env.getOrDefault("KAFKA_PARTITIONS", "16")))) {
      System.out.println("Ledger migrations and immutable topology checks completed.");
    }
  }
}
