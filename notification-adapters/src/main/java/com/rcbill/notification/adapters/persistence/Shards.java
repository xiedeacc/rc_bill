package com.rcbill.notification.adapters.persistence;

import com.rcbill.notification.domain.Routing;
import com.zaxxer.hikari.*;
import java.util.*;
import java.util.function.Function;
import org.apache.ibatis.session.LocalCacheScope;
import org.mybatis.spring.*;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.flywaydb.core.Flyway;

/** Immutable topology per deployment. No cross-shard transaction and no implicit reshard. */
public final class Shards implements AutoCloseable {
    public record Shard(SqlSessionTemplate sessions, TransactionTemplate transactions, HikariDataSource pool) {
        public <T> T transaction(Function<LedgerMapper,T> action) {
            return transactions.execute(status -> action.apply(sessions.getMapper(LedgerMapper.class)));
        }
        public LedgerMapper mapper() { return sessions.getMapper(LedgerMapper.class); }
    }
    private final List<Shard> shards=new ArrayList<>();
    private final int partitions;
    public Shards(List<String> urls,String user,String password,boolean migrate,int partitions) {
        if(urls.isEmpty() || urls.size()>4096 || partitions<urls.size() || partitions>4096
                || 4096%partitions!=0 || partitions%urls.size()!=0) throw new IllegalArgumentException("Incompatible immutable shard/partition topology");
        this.partitions=partitions;
        try {
            for(String url:urls) {
                HikariConfig h=new HikariConfig();h.setJdbcUrl(url);h.setUsername(user);h.setPassword(password);
                h.setMaximumPoolSize(8);h.setMinimumIdle(0);h.setConnectionTimeout(3000);h.setPoolName("ledger-"+shards.size());
                h.setConnectionInitSql("SET statement_timeout='5s'");
                HikariDataSource ds=new HikariDataSource(h);
                try {
                    if(migrate)Flyway.configure().dataSource(ds).locations("classpath:db/migration").load().migrate();
                    var cfg=new org.apache.ibatis.session.Configuration();cfg.setCacheEnabled(false);cfg.setLocalCacheScope(LocalCacheScope.STATEMENT);
                    cfg.addMapper(LedgerMapper.class);
                    var bean=new SqlSessionFactoryBean();bean.setDataSource(ds);bean.setConfiguration(cfg);
                    var shard=new Shard(new SqlSessionTemplate(bean.getObject()),new TransactionTemplate(new DataSourceTransactionManager(ds)),ds);
                    shards.add(shard);
                    shard.transaction(m -> {
                        m.initializeTopology(Db.map("shards",urls.size(),"partitions",partitions));
                        var row=m.topology();
                        if(Db.integer(row,"shard_count")!=urls.size() || Db.integer(row,"partition_count")!=partitions)
                            throw new IllegalStateException("Topology changed: migrate explicitly before starting");
                        return null;
                    });
                } catch(Exception error) {ds.close();throw error;}
            }
        } catch(Exception error) {close();throw new IllegalStateException("Cannot initialize ledger shards",error);}
    }
    public Shard slot(int slot) {if(slot<0||slot>4095)throw new IllegalArgumentException("slot");return shards.get(slot%shards.size());}
    public Shard task(UUID id) {return slot(Routing.slot(id));}
    public List<Shard> all() {return List.copyOf(shards);}
    public int partitions(){return partitions;}
    @Override public void close(){shards.forEach(s->s.pool().close());}
}
