package com.rcbill.notification.adapters.persistence;

import com.rcbill.notification.application.port.OutboxStore;
import com.rcbill.notification.domain.DispatchEvent;
import java.time.*;
import java.util.*;
import static com.rcbill.notification.adapters.persistence.Db.*;

public final class PgOutboxStore implements OutboxStore {
    private final Shards shards;
    public PgOutboxStore(Shards shards){this.shards=shards;}
    @Override public List<Publication> leaseDue(int shard,int lane,int limit,Duration duration){
        return shards.all().get(shard).transaction(m->{
            var rows=m.dueOutbox(limit);Instant until=m.now().plus(duration);var result=new ArrayList<Publication>();
            for(var row:rows){UUID token=UUID.randomUUID();m.leaseOutbox(map("event",row.get("event_id"),"token",token,"until",until));
                var event=new DispatchEvent(1,id(row.get("event_id")),id(row.get("notification_id")),integer(row,"slot"),number(row,"schedule_epoch"),(String)row.get("destination_id"),instant(row.get("created_at")),instant(row.get("expires_at")));
                result.add(new Publication(event,(String)row.get("event_type"),token,until));}
            return List.copyOf(result);
        });
    }
    @Override public boolean markPublished(Publication p){return shards.slot(p.event().logicalSlot()).transaction(m->m.published(map("event",p.event().eventId(),"token",p.leaseToken()))==1);}
    @Override public boolean retryPublication(Publication p,Instant at,String error){return shards.slot(p.event().logicalSlot()).transaction(m->m.republish(map("event",p.event().eventId(),"token",p.leaseToken(),"available",at))==1);}
    @Override public int recoverExpiredPublicationLeases(int shard,int limit){return shards.all().get(shard).transaction(m->m.recoverOutbox(limit));}
}
