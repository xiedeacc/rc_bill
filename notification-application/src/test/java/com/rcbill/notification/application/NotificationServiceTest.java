package com.rcbill.notification.application;
import com.rcbill.notification.application.port.*;
import com.rcbill.notification.domain.*;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class NotificationServiceTest {
    final NotificationUseCases.Caller caller=new NotificationUseCases.Caller("t","c","s");
    final SubmissionRequest input=new SubmissionRequest("crm-test",1,"text/plain","value",Map.of(),60).validated();
    @Test void duplicateResolutionPrecedesDisabledDestination() {
        var store=mock(NotificationStore.class);var catalog=mock(DestinationCatalog.class);
        var accepted=new NotificationUseCases.Acceptance(UUID.randomUUID(),Instant.EPOCH,false);
        when(store.findIdempotency(caller,"submit","event-key")).thenReturn(Optional.of(new NotificationStore.IdempotencyHit(input.fingerprint(),accepted)));
        var result=new NotificationService(store,catalog).submit(caller,"event-key",input);
        assertThat(result.notificationId()).isEqualTo(accepted.notificationId());assertThat(result.replayed()).isTrue();
        verifyNoInteractions(catalog);verify(store,never()).accept(any(),any(),any(),any(),any());
    }
    @Test void changedFingerprintConflictsWithoutCreatingTask() {
        var store=mock(NotificationStore.class);
        when(store.findIdempotency(caller,"submit","event-key")).thenReturn(Optional.of(new NotificationStore.IdempotencyHit("different",new NotificationUseCases.Acceptance(UUID.randomUUID(),Instant.EPOCH,false))));
        assertThatThrownBy(()->new NotificationService(store,mock(DestinationCatalog.class)).submit(caller,"event-key",input))
                .isInstanceOfSatisfying(ServiceFailure.class,e->assertThat(e.status()).isEqualTo(409));
    }
    @Test void forbiddenDestinationHeaderNeverReachesStore() {
        var store=mock(NotificationStore.class);var catalog=mock(DestinationCatalog.class);var snapshot=mock(DestinationCatalog.Snapshot.class);
        when(store.findIdempotency(any(),any(),any())).thenReturn(Optional.empty());
        when(catalog.resolveAuthorized(caller,"crm-test",1)).thenReturn(snapshot);
        when(snapshot.contentTypes()).thenReturn(Set.of("text/plain"));when(snapshot.allowedDynamicHeaders()).thenReturn(Set.of());
        var request=new SubmissionRequest("crm-test",1,"text/plain","",Map.of("X-NOT-ALLOWED","x"),60);
        assertThatThrownBy(()->new NotificationService(store,catalog).submit(caller,"event-key",request)).isInstanceOf(ServiceFailure.class);
        verify(store,never()).accept(any(),any(),any(),any(),any());
    }
    @Test void malformedIdentityAndPaginationFailBeforeRepository() {
        var store=mock(NotificationStore.class);var service=new NotificationService(store,mock(DestinationCatalog.class));
        assertThatThrownBy(()->service.submit(new NotificationUseCases.Caller("","c","s"),"event-key",input)).isInstanceOf(ServiceFailure.class);
        assertThatThrownBy(()->service.attempts(caller,UUID.randomUUID(),-1,1000)).isInstanceOf(ServiceFailure.class);
        verifyNoInteractions(store);
    }
}
