package com.rcbill.notification.adapters.persistence;

import com.rcbill.notification.domain.Routing;
import com.zaxxer.hikari.*;
import java.util.*;
import java.util.function.Function;
import org.apache.ibatis.session.LocalCacheScope;
import org.flywaydb.core.Flyway;
import org.mybatis.spring.*;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Immutable topology per deployment. No cross-shard transaction and no implicit reshard. */
public final class Shards implements AutoCloseable {
  public record Shard(
      SqlSessionTemplate sessions, TransactionTemplate transactions, HikariDataSource pool) {
    public <T> T transaction(Function<LedgerMapper, T> action) {
      try (var total =
          com.rcbill.notification.adapters.observability.StageTiming.start("transaction")) {
        return transactions.execute(
            status -> {
              try (var work =
                  com.rcbill.notification.adapters.observability.StageTiming.start(
                      "transaction.work")) {
                return action.apply(sessions.getMapper(LedgerMapper.class));
              }
            });
      }
    }

    public LedgerMapper mapper() {
      return sessions.getMapper(LedgerMapper.class);
    }
  }

  private final List<Shard> shards = new ArrayList<>();
  private final int partitions;

  public Shards(List<String> urls, String user, String password, boolean migrate, int partitions) {
    if (urls.isEmpty()
        || urls.size() > 64
        || partitions < urls.size()
        || partitions > 4096
        || 4096 % partitions != 0
        || partitions % urls.size() != 0)
      throw new IllegalArgumentException("Incompatible immutable shard/partition topology");
    this.partitions = partitions;
    try {
      for (String url : urls) {
        HikariConfig h = new HikariConfig();
        h.setJdbcUrl(url);
        h.setUsername(user);
        h.setPassword(password);
        h.setMaximumPoolSize(Math.min(8, Math.max(1, 64 / urls.size())));
        h.setMinimumIdle(0);
        h.setConnectionTimeout(3000);
        h.setPoolName("ledger-" + shards.size());
        h.setConnectionInitSql(
            migrate ? "SET statement_timeout='30min'" : "SET statement_timeout='5s'");
        h.addDataSourceProperty("options", "-c lock_timeout=5000");
        h.addDataSourceProperty("prepareThreshold", "0");
        h.addDataSourceProperty("connectTimeout", "3");
        h.addDataSourceProperty("socketTimeout", migrate ? "1805" : "5");
        HikariDataSource ds = new HikariDataSource(h);
        try {
          if (migrate)
            Flyway.configure()
                .dataSource(ds)
                .configuration(Map.of("flyway.postgresql.transactional.lock", "false"))
                .locations("classpath:db/migration")
                .load()
                .migrate();
          var cfg = new org.apache.ibatis.session.Configuration();
          cfg.setCacheEnabled(false);
          cfg.setLocalCacheScope(LocalCacheScope.STATEMENT);
          cfg.addInterceptor(new com.rcbill.notification.adapters.observability.LedgerTiming());
          cfg.addMapper(LedgerMapper.class);
          var bean = new SqlSessionFactoryBean();
          bean.setDataSource(ds);
          bean.setConfiguration(cfg);
          var shard =
              new Shard(
                  new SqlSessionTemplate(bean.getObject()),
                  new TransactionTemplate(new DataSourceTransactionManager(ds)),
                  ds);
          shards.add(shard);
          int physicalId = shards.size() - 1;
          shard.transaction(
              m -> {
                m.initializeTopology(
                    Db.map(
                        "shards", urls.size(), "partitions", partitions, "physicalId", physicalId));
                var row = m.topology();
                if (Db.integer(row, "shard_count") != urls.size()
                    || Db.integer(row, "partition_count") != partitions)
                  throw new IllegalStateException(
                      "Topology changed: migrate explicitly before starting");
                if (row.get("physical_shard_id") == null
                    || Db.integer(row, "physical_shard_id") != physicalId)
                  throw new IllegalStateException(
                      "Physical shard identity missing or reordered; explicit mapping migration"
                          + " required");
                return null;
              });
        } catch (Exception error) {
          ds.close();
          throw error;
        }
      }
    } catch (Exception error) {
      close();
      throw new IllegalStateException("Cannot initialize ledger shards", error);
    }
  }

  public Shard slot(int slot) {
    if (slot < 0 || slot > 4095) throw new IllegalArgumentException("slot");
    return shards.get(slot % shards.size());
  }

  public Shard task(UUID id) {
    return slot(Routing.slot(id));
  }

  public List<Shard> all() {
    return List.copyOf(shards);
  }

  public int partitions() {
    return partitions;
  }

  public int maintainAvailable(Function<Shard, Integer> action) {
    int completed = 0, total = 0, index = 0;
    for (var shard : shards) {
      try {
        total += action.apply(shard);
        completed++;
      } catch (RuntimeException failure) {
        org.slf4j.LoggerFactory.getLogger(Shards.class)
            .warn(
                "ledger_maintenance_unavailable shard={} cause={}",
                index,
                failure.getClass().getSimpleName());
      }
      index++;
    }
    if (completed == 0) throw new IllegalStateException("All ledger shards unavailable");
    return total;
  }

  @Override
  public void close() {
    shards.forEach(s -> s.pool().close());
  }
}
