package com.rcbill.notification.worker;
import com.rcbill.notification.application.*;
import com.rcbill.notification.application.port.*;
import com.rcbill.notification.adapters.config.ConfiguredCatalog;
import com.rcbill.notification.domain.*;
import io.github.resilience4j.circuitbreaker.*;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.RejectedExecutionException;

public final class DeliveryDispatcher implements AutoCloseable {
    private final DeliveryQueue queue;private final HttpTransport transport;private final SecretProvider secrets;
    private final DispatchAdmission admission;private final ConfiguredCatalog catalog;private final BoundedExecution execution;
    private final Clock clock;private final MeterRegistry meters;
    private final CircuitBreakerRegistry breakers=CircuitBreakerRegistry.of(CircuitBreakerConfig.custom().slidingWindowSize(50).minimumNumberOfCalls(20).failureRateThreshold(50).waitDurationInOpenState(Duration.ofSeconds(30)).permittedNumberOfCallsInHalfOpenState(3).build());
    public DeliveryDispatcher(DeliveryQueue queue,HttpTransport transport,SecretProvider secrets,DispatchAdmission admission,ConfiguredCatalog catalog,int maximum,Clock clock,MeterRegistry meters){
        this.queue=queue;this.transport=transport;this.secrets=secrets;this.admission=admission;this.catalog=catalog;execution=new BoundedExecution(maximum);this.clock=clock;this.meters=meters;
        meters.gauge("notification.http.inflight",execution,BoundedExecution::inFlight);
    }
    public void handle(DispatchEvent event){
        var slot=execution.reserve().orElseThrow(()->new RejectedExecutionException("Local capacity exhausted"));
        DispatchAdmission.Permit permit=null;CircuitBreaker breaker=breakers.circuitBreaker(event.destinationId());boolean breakerPermit=false,handed=false;
        try{
            var active=catalog.all().stream().filter(d->d.id().equals(event.destinationId())&&d.version()==event.destinationVersion()&&d.enabled()).findFirst();
            if(active.isEmpty()){queue.defer(event,clock.instant().plusSeconds(60),"DESTINATION_PAUSED");return;}
            if(!breaker.tryAcquirePermission()){queue.defer(event,clock.instant().plusSeconds(30),"CIRCUIT_OPEN");return;}breakerPermit=true;
            String budget=active.get().credentialRef()!=null?active.get().credentialRef():active.get().endpoint().getAuthority();
            permit=admission.tryAcquire(budget).orElse(null);
            if(permit==null){queue.defer(event,clock.instant().plusSeconds(5),"ADMISSION_UNAVAILABLE");return;}
            var handoff=queue.claim(event,Duration.ofSeconds(60));
            if(handoff.disposition()!=DeliveryQueue.Disposition.CLAIMED)return;
            var granted=permit;var lease=Objects.requireNonNull(handoff.lease());
            slot.execute(()->deliver(lease,granted,breaker));handed=true;
        }finally{if(!handed){slot.close();if(permit!=null)permit.close();if(breakerPermit)breaker.releasePermission();}}
    }
    private void deliver(DeliveryQueue.Lease lease,DispatchAdmission.Permit permit,CircuitBreaker breaker){
        boolean recorded=false;
        try{
            var payload=queue.loadOwnedPayload(lease);if(payload.isEmpty()){breaker.releasePermission();return;}
            var p=payload.get();var d=p.destination();
            if(!catalog.enabled(d.id(),d.version())||!permit.validUntil().isAfter(clock.instant().plusSeconds(d.policy().timeoutSeconds()))){
                queue.complete(lease,new DeliveryQueue.Completion(NotificationStatus.RETRY_WAIT,AttemptOutcome.UNKNOWN,clock.instant().plusSeconds(5),null,"ADMISSION_EXPIRED",0,null));breaker.releasePermission();return;
            }
            Map<String,String> headers=new TreeMap<>(String.CASE_INSENSITIVE_ORDER);headers.putAll(d.staticHeaders());headers.putAll(p.dynamicHeaders());headers.put("Content-Type",p.contentType()+"; charset=UTF-8");
            if(d.idempotencyHeader()!=null)headers.put(d.idempotencyHeader(),lease.deliveryKey());
            if(d.credentialRef()!=null)try(var secret=secrets.resolve(d.credentialRef())){headers.put(d.credentialHeader(),d.credentialPrefix()+new String(secret.value()));}
            var result=transport.send(new HttpTransport.PreparedRequest(){
                public URI endpoint(){return d.endpoint();}public String method(){return d.method();}public Map<String,String> headers(){return headers;}
                public byte[] body(){return p.body();}public Duration totalTimeout(){return Duration.ofSeconds(d.policy().timeoutSeconds());}public int maximumResponseBytes(){return 65536;}
            });
            if(result.transportErrorCode()!=null||(result.httpStatus()!=null&&result.httpStatus()>=500))breaker.onError(result.elapsedMs(),java.util.concurrent.TimeUnit.MILLISECONDS,new IllegalStateException("Provider failure"));
            else breaker.onSuccess(result.elapsedMs(),java.util.concurrent.TimeUnit.MILLISECONDS);
            recorded=true;
            var decision=RetryPolicy.decide(clock.instant(),lease.expiresAt(),lease.attemptNo(),d.policy().maxAttempts(),result.httpStatus(),result.retryAfter(),result.transportErrorCode(),()->java.util.concurrent.ThreadLocalRandom.current().nextDouble());
            queue.complete(lease,new DeliveryQueue.Completion(decision.status(),decision.outcome(),decision.nextAttemptAt(),result.httpStatus(),result.transportErrorCode()!=null?result.transportErrorCode():decision.status()==NotificationStatus.SUCCEEDED?null:"HTTP_"+result.httpStatus(),result.elapsedMs(),result.providerRequestId()));
            meters.counter("notification.attempts","outcome",decision.outcome().name()).increment();
        }catch(RuntimeException failure){meters.counter("notification.writeback.failure").increment();if(!recorded)breaker.releasePermission();/* Lease recovery retains responsibility; never resend here. */}
        finally{permit.close();}
    }
    public int inFlight(){return execution.inFlight();}
    @Override public void close(){execution.close();}
}
