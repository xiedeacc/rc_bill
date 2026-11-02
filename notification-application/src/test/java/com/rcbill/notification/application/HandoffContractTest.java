package com.rcbill.notification.application;
import com.rcbill.notification.application.port.DeliveryQueue;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
class HandoffContractTest {
    @Test void claimedRequiresLeaseAndDuplicatesCannotCarryExecutableLease(){
        var lease=new DeliveryQueue.Lease("t",UUID.randomUUID(),1,0,UUID.randomUUID(),1,Instant.EPOCH,Instant.EPOCH,"stable");
        assertThatIllegalArgumentException().isThrownBy(()->new DeliveryQueue.Handoff(DeliveryQueue.Disposition.CLAIMED,null));
        assertThatIllegalArgumentException().isThrownBy(()->new DeliveryQueue.Handoff(DeliveryQueue.Disposition.ALREADY_DURABLE,lease));
        assertThatIllegalArgumentException().isThrownBy(()->new DeliveryQueue.Handoff(null,null));
    }
}
