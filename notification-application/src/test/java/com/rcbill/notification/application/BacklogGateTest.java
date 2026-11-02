package com.rcbill.notification.application;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
class BacklogGateTest {
    @Test void samplesWithExplicitBoundedStalenessAndCanRecover(){
        var time=new AtomicLong();var count=new AtomicLong(1);var probes=new AtomicLong();var gate=new BacklogGate(2,Duration.ofSeconds(1),time::get);
        java.util.function.LongSupplier probe=()->{probes.incrementAndGet();return count.get();};
        gate.check(0,probe);count.set(2);gate.check(0,probe);assertThat(probes.get()).isEqualTo(1);
        time.set(1_000_000_001L);assertThatThrownBy(()->gate.check(0,probe)).isInstanceOf(ServiceFailure.class);
        count.set(0);assertThatThrownBy(()->gate.check(0,probe)).isInstanceOf(ServiceFailure.class);
        time.set(2_000_000_002L);assertThatCode(()->gate.check(0,probe)).doesNotThrowAnyException();
    }
    @Test void failureToReadCapacityNeverCreatesAnAllowCache(){
        var gate=new BacklogGate(1,Duration.ofSeconds(1),()->0);
        assertThatIllegalStateException().isThrownBy(()->gate.check(0,()->{throw new IllegalStateException("database unavailable");}));
        assertThatThrownBy(()->gate.check(0,()->1)).isInstanceOf(ServiceFailure.class);
    }
}
