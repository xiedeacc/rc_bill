package com.rcbill.notification.worker;
import com.rcbill.notification.application.port.*;
import com.rcbill.notification.adapters.config.*;
import com.rcbill.notification.domain.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
class DeliveryDispatcherTest {
    final Instant now=Instant.parse("2026-11-02T12:00:00Z");
    final Destination destination=new Destination("crm-test",1,Set.of("t"),Set.of("c"),true,URI.create("https://example.com/notify"),"POST",Set.of("text/plain"),Set.of(),Map.of("idempotency-key","incorrect-static-value"),null,"Idempotency-Key",new DestinationCatalog.DurationPolicy(2,20,86400));
    DispatchEvent event(){var id=Routing.newId(3);return new DispatchEvent(1,UUID.randomUUID(),id,3,0,"crm-test",now,now.plusSeconds(60));}
    @Test void duplicateAndDeniedAdmissionDoNotSendOrSpendAttempts(){
        var q=mock(DeliveryQueue.class);var http=mock(HttpTransport.class);var admission=mock(DispatchAdmission.class);var permit=mock(DispatchAdmission.Permit.class);var event=event();
        when(admission.tryAcquire(any())).thenReturn(Optional.of(permit));when(q.claim(any(),any())).thenReturn(new DeliveryQueue.Handoff(DeliveryQueue.Disposition.ALREADY_DURABLE,null));
        try(var d=new DeliveryDispatcher(q,http,mock(SecretProvider.class),admission,new ConfiguredCatalog(List.of(destination)),1,Clock.fixed(now,ZoneOffset.UTC),new SimpleMeterRegistry())){
            d.handle(event);verify(permit).close();assertThat(d.inFlight()).isZero();verifyNoInteractions(http);
            reset(q);when(admission.tryAcquire(any())).thenReturn(Optional.empty());d.handle(event);verify(q).defer(eq(event),eq(now.plusSeconds(5)),eq("ADMISSION_UNAVAILABLE"));verify(q,never()).claim(any(),any());
        }
    }
    @Test void stableKeyOverridesCaseVariantAndResultIsPersisted()throws Exception{
        var q=mock(DeliveryQueue.class);var http=mock(HttpTransport.class);var admission=mock(DispatchAdmission.class);var permit=mock(DispatchAdmission.Permit.class);var event=event();var done=new CountDownLatch(1);
        var lease=new DeliveryQueue.Lease("t",event.notificationId(),3,0,UUID.randomUUID(),1,now.plusSeconds(60),now.plusSeconds(60),"stable-delivery-key");
        when(admission.tryAcquire(any())).thenReturn(Optional.of(permit));when(permit.validUntil()).thenReturn(now.plusSeconds(60));doAnswer(i->{done.countDown();return null;}).when(permit).close();
        when(q.claim(any(),any())).thenReturn(new DeliveryQueue.Handoff(DeliveryQueue.Disposition.CLAIMED,lease));
        when(q.loadOwnedPayload(lease)).thenReturn(Optional.of(new DeliveryQueue.DispatchPayload(){public DestinationCatalog.Snapshot destination(){return destination;}public String contentType(){return "text/plain";}public byte[] body(){return new byte[0];}public Map<String,String> dynamicHeaders(){return Map.of();}}));
        var result=mock(HttpTransport.Result.class);when(result.httpStatus()).thenReturn(204);when(http.send(any())).thenAnswer(i->{HttpTransport.PreparedRequest request=i.getArgument(0);assertThat(request.headers().get("Idempotency-Key")).isEqualTo("stable-delivery-key");assertThat(request.headers().keySet().stream().filter(k->k.equalsIgnoreCase("idempotency-key")).count()).isEqualTo(1);return result;});
        try(var d=new DeliveryDispatcher(q,http,mock(SecretProvider.class),admission,new ConfiguredCatalog(List.of(destination)),1,Clock.fixed(now,ZoneOffset.UTC),new SimpleMeterRegistry())){d.handle(event);assertThat(done.await(5,TimeUnit.SECONDS)).isTrue();}
        verify(q).complete(eq(lease),argThat(c->c.nextStatus()==NotificationStatus.SUCCEEDED));verify(http,times(1)).send(any());
    }
    @Test void enabledNewVersionDoesNotSpendAttemptsOnPausedOldVersion(){
        var q=mock(DeliveryQueue.class);var http=mock(HttpTransport.class);var admission=mock(DispatchAdmission.class);
        var next=new Destination("crm-test",2,Set.of("t"),Set.of("c"),true,URI.create("https://example.com/next"),"POST",Set.of("text/plain"),Set.of(),Map.of(),null,"Idempotency-Key",new DestinationCatalog.DurationPolicy(2,20,86400));
        try(var d=new DeliveryDispatcher(q,http,mock(SecretProvider.class),admission,new ConfiguredCatalog(List.of(next)),1,Clock.fixed(now,ZoneOffset.UTC),new SimpleMeterRegistry())){var old=event();d.handle(old);verify(q).defer(eq(old),eq(now.plusSeconds(60)),eq("DESTINATION_PAUSED"));verify(q,never()).claim(any(),any());verifyNoInteractions(http,admission);}
    }
}
