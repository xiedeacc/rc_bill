package com.rcbill.notification.application;
import java.time.Duration;
import java.util.function.LongSupplier;
import java.util.concurrent.ConcurrentHashMap;
public final class BacklogGate {
    private record Sample(long at,boolean full){}
    private final ConcurrentHashMap<Integer,Sample> samples=new ConcurrentHashMap<>();
    private final long limit,ttl;private final LongSupplier nanos;
    public BacklogGate(long limit,Duration ttl,LongSupplier nanos){if(limit<1||ttl.isNegative())throw new IllegalArgumentException("capacity configuration");this.limit=limit;this.ttl=ttl.toNanos();this.nanos=nanos;}
    public void check(int shard,LongSupplier count){
        var sample=samples.compute(shard,(key,old)->{long now=nanos.getAsLong();return old!=null&&now-old.at()<ttl?old:new Sample(now,count.getAsLong()>=limit);});
        if(sample.full())throw new ServiceFailure(503,"BACKLOG_CAPACITY");
    }
}
