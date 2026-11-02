package com.rcbill.notification.adapters.persistence;

import com.rcbill.notification.application.ServiceFailure;
import com.rcbill.notification.application.port.*;
import com.rcbill.notification.adapters.config.Destination;
import com.rcbill.notification.adapters.security.PayloadCipher;
import com.rcbill.notification.domain.*;
import tools.jackson.databind.json.JsonMapper;
import java.time.*;
import java.util.*;
import static com.rcbill.notification.adapters.persistence.Db.*;

public final class PgDeliveryQueue implements DeliveryQueue {
    private final Shards shards;private final PayloadCipher cipher;private final JsonMapper json;
    public PgDeliveryQueue(Shards shards,PayloadCipher cipher,JsonMapper json){this.shards=shards;this.cipher=cipher;this.json=json;}
    private Destination destination(Map<String,Object> row){return json.readValue((String)row.get("snapshot"),Destination.class);}
    private Map<String,Object> current(LedgerMapper m,DispatchEvent event){
        if(event.schemaVersion()!=1||event.logicalSlot()<0||event.logicalSlot()>4095||event.scheduleEpoch()<0||Routing.slot(event.notificationId())!=event.logicalSlot())throw new ServiceFailure(400,"INVALID_EVENT");
        var row=m.lockTask(event.notificationId());
        if(row==null)throw new ServiceFailure(503,"TASK_MISSING");
        if(!Objects.equals(row.get("destination_id"),event.destinationId()))throw new ServiceFailure(400,"EVENT_DESTINATION_MISMATCH");
        if(event.scheduleEpoch()>number(row,"schedule_epoch"))throw new ServiceFailure(503,"FUTURE_EPOCH");
        return row;
    }
    private boolean covered(Map<String,Object> row,DispatchEvent event,Instant now){
        if(event.scheduleEpoch()<number(row,"schedule_epoch")||Set.of("SUCCEEDED","DEAD","EXPIRED").contains(row.get("status")))return true;
        if("IN_FLIGHT".equals(row.get("status"))){
            if(instant(row.get("lease_until")).isAfter(now))return true;
            throw new ServiceFailure(503,"LEASE_RECOVERY_PENDING");
        }
        return false;
    }
    @Override public Handoff claim(DispatchEvent event,Duration duration){return shards.slot(event.logicalSlot()).transaction(m->{
        var row=current(m,event);Instant now=m.now();
        if(covered(row,event,now))return new Handoff(Disposition.ALREADY_DURABLE,null);
        var d=destination(row);
        if(!instant(row.get("expires_at")).isAfter(now.plusSeconds(d.policy().timeoutSeconds()))){
            transition(m,row,NotificationStatus.EXPIRED,now,"TTL_EXPIRED");return new Handoff(Disposition.ALREADY_DURABLE,null);
        }
        if(integer(row,"attempt_count")>=d.policy().maxAttempts()){
            transition(m,row,NotificationStatus.DEAD,now,"ATTEMPTS_EXHAUSTED");return new Handoff(Disposition.ALREADY_DURABLE,null);
        }
        if(instant(row.get("next_attempt_at")).isAfter(now)){
            transition(m,row,NotificationStatus.RETRY_WAIT,instant(row.get("next_attempt_at")),"NOT_DUE");return new Handoff(Disposition.DEFERRED,null);
        }
        UUID token=UUID.randomUUID();Instant until=now.plus(duration);int attempt=integer(row,"attempt_count")+1;
        int claimed=m.claim(map("id",event.notificationId(),"epoch",event.scheduleEpoch(),"token",token,"until",until));
        if(claimed!=1)throw new IllegalStateException("Claim invariant violated");
        m.start(map("id",event.notificationId(),"attempt",attempt,"token",token));
        return new Handoff(Disposition.CLAIMED,new Lease((String)row.get("tenant_id"),event.notificationId(),event.logicalSlot(),event.scheduleEpoch(),token,attempt,until,instant(row.get("expires_at")),(String)row.get("delivery_key")));
    });}
    @Override public Handoff defer(DispatchEvent event,Instant next,String reason){return shards.slot(event.logicalSlot()).transaction(m->{
        var row=current(m,event);Instant now=m.now();
        if(covered(row,event,now))return new Handoff(Disposition.ALREADY_DURABLE,null);
        transition(m,row,next.isBefore(instant(row.get("expires_at")))?NotificationStatus.RETRY_WAIT:NotificationStatus.EXPIRED,next,reason);
        return new Handoff(Disposition.DEFERRED,null);
    });}
    private boolean owns(Map<String,Object> row,Lease lease,Instant now){return row!=null&&"IN_FLIGHT".equals(row.get("status"))&&Objects.equals(row.get("lease_token"),lease.token())&&Objects.equals(row.get("tenant_id"),lease.tenantId())&&instant(row.get("lease_until")).isAfter(now);}
    @Override public Optional<DispatchPayload> loadOwnedPayload(Lease lease){return shards.slot(lease.logicalSlot()).transaction(m->{
        var row=m.task(lease.notificationId());if(!owns(row,lease,m.now()))return Optional.empty();
        byte[] encrypted=m.payload(lease.notificationId());if(encrypted==null)throw new IllegalStateException("Active payload missing");
        var body=json.readValue(cipher.decrypt(lease.notificationId(),encrypted),PgNotificationStore.Body.class);var d=destination(row);
        return Optional.of(new DispatchPayload(){
            public DestinationCatalog.Snapshot destination(){return d;}
            public String contentType(){return body.contentType();}
            public byte[] body(){return body.body().getBytes(java.nio.charset.StandardCharsets.UTF_8);}
            public Map<String,String> dynamicHeaders(){return Map.copyOf(body.headers());}
        });
    });}
    @Override public boolean complete(Lease lease,Completion completion){return shards.slot(lease.logicalSlot()).transaction(m->{
        var row=m.lockTask(lease.notificationId());if(!owns(row,lease,m.now()))return false;
        if(!Set.of(NotificationStatus.SUCCEEDED,NotificationStatus.RETRY_WAIT,NotificationStatus.DEAD,NotificationStatus.EXPIRED).contains(completion.nextStatus()))throw new IllegalArgumentException("Illegal completion state");
        int updated=m.finish(map("id",lease.notificationId(),"token",lease.token(),"outcome",completion.outcome().name(),"http",completion.httpStatus(),"error",completion.errorCode(),"elapsed",completion.elapsedMs(),"provider",completion.providerRequestId()));
        if(updated!=1)throw new IllegalStateException("Attempt invariant violated");
        transition(m,row,completion.nextStatus(),completion.nextAttemptAt()==null?m.now():completion.nextAttemptAt(),completion.errorCode());return true;
    });}
    private void transition(LedgerMapper m,Map<String,Object> row,NotificationStatus status,Instant next,String error){
        UUID task=id(row.get("id"));boolean retry=status==NotificationStatus.RETRY_WAIT;long epoch=number(row,"schedule_epoch")+(retry?1:0);
        m.transition(map("id",task,"status",status.name(),"epoch",epoch,"next",next,"error",error,"completed",retry?null:m.now()));
        if(!retry)m.active(map("root",row.get("root_id"),"active",null));
        if(retry||status==NotificationStatus.DEAD||status==NotificationStatus.EXPIRED)
            PgNotificationStore.event(m,task,integer(row,"slot"),epoch,retry?"DISPATCH":"DEAD",retry?next:m.now());
    }
    @Override public int recoverExpiredLeases(int batch){int count=0;for(var shard:shards.all())count+=shard.transaction(m->{
        var rows=m.abandoned(batch);
        for(var row:rows){
            var decision=RetryPolicy.decide(m.now(),instant(row.get("expires_at")),integer(row,"attempt_count"),destination(row).policy().maxAttempts(),null,null,"IO_UNKNOWN",()->0.5);
            m.finish(map("id",row.get("id"),"token",row.get("lease_token"),"outcome","UNKNOWN","http",null,"error","LEASE_EXPIRED","elapsed",null,"provider",null));
            transition(m,row,decision.status(),decision.nextAttemptAt()==null?m.now():decision.nextAttemptAt(),"LEASE_EXPIRED");
        }return rows.size();
    });return count;}
    @Override public int expireQueued(int batch){int count=0;for(var shard:shards.all())count+=shard.transaction(m->{var rows=m.expired(batch);for(var row:rows)transition(m,row,NotificationStatus.EXPIRED,m.now(),"TTL_EXPIRED");return rows.size();});return count;}
}
