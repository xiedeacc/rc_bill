package com.rcbill.notification.application;
import com.rcbill.notification.application.port.*;
import com.rcbill.notification.domain.NotificationView;
import com.rcbill.notification.domain.Routing;
import java.util.*;
public final class NotificationService implements NotificationUseCases {
    private final NotificationStore store;private final DestinationCatalog catalog;
    public NotificationService(NotificationStore store,DestinationCatalog catalog) {this.store=store;this.catalog=catalog;}
    public Acceptance submit(Caller c,String key,Submission s){
        identity(c);SubmissionRequest.validateKey(key);
        var request=new SubmissionRequest(s.destinationId(),s.destinationVersion(),s.contentType(),s.body(),s.headers(),s.ttlSeconds()).validated();
        String fingerprint=request.fingerprint();
        var hit=store.findIdempotency(c,"submit",key);
        if(hit.isPresent())return existing(hit.get(),fingerprint);
        var d=catalog.resolveAuthorized(c,request.destinationId(),request.destinationVersion());
        if(!d.contentTypes().contains(request.contentType())||!d.allowedDynamicHeaders().containsAll(request.headers().keySet())
                || (d.idempotencyHeader()!=null&&request.headers().containsKey(d.idempotencyHeader().toLowerCase(Locale.ROOT))))
            throw new ServiceFailure(422,"INVALID_DESTINATION_REQUEST");
        return store.accept(c,key,fingerprint,request,d);
    }
    public Optional<NotificationView> get(Caller c,UUID id){identity(c);return store.find(c,id);}
    public AttemptPage attempts(Caller c,UUID id,int after,int limit){
        identity(c);if(after<0||limit<1||limit>100)throw new ServiceFailure(400,"INVALID_PAGINATION");
        return store.attempts(c,id,after,limit);
    }
    public Acceptance replay(Caller c,UUID id,String key,Replay replay){
        identity(c);SubmissionRequest.validateKey(key);
        if(replay==null||replay.reason()==null||replay.reason().isBlank()||replay.reason().length()>512)throw new ServiceFailure(400,"INVALID_REPLAY_REASON");
        String fingerprint=java.util.HexFormat.of().formatHex(Routing.digest(replay.reason(),Boolean.toString(replay.acknowledgeDuplicateRisk())));
        var hit=store.findIdempotency(c,"replay:"+id,key);
        return hit.isPresent()?existing(hit.get(),fingerprint):store.replay(c,id,key,fingerprint,replay);
    }
    public static Acceptance existing(NotificationStore.IdempotencyHit hit,String fingerprint){
        if(!hit.fingerprint().equals(fingerprint))throw new ServiceFailure(409,"IDEMPOTENCY_CONFLICT");
        return new Acceptance(hit.acceptance().notificationId(),hit.acceptance().acceptedAt(),true);
    }
    private static void identity(Caller c){
        if(c==null||c.tenantId()==null||c.tenantId().isBlank()||c.clientId()==null||c.clientId().isBlank()||c.subject()==null||c.subject().isBlank()
                ||c.tenantId().length()>128||c.clientId().length()>128)throw new ServiceFailure(401,"INVALID_CALLER");
    }
}
