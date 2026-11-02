package com.rcbill.notification.application.port;

import com.rcbill.notification.domain.DispatchEvent;
import java.util.concurrent.CompletionStage;

/** Kafka adapter: the stage completes only after broker acknowledgement, never on buffer enqueue. */
public interface DispatchPublisher {
    /** Uses notificationId as key and topology-v1 partition = logicalSlot % 2048. */
    CompletionStage<Void> publish(DispatchEvent event, String eventType);
}
