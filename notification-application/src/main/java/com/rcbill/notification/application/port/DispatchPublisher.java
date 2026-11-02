package com.rcbill.notification.application.port;

import com.rcbill.notification.domain.DispatchEvent;
import java.util.concurrent.CompletionStage;

/**
 * Kafka adapter: the stage completes only after broker acknowledgement, never on buffer enqueue.
 */
public interface DispatchPublisher {
  /**
   * Uses notificationId as key and logicalSlot modulo the deployment's immutable partition count.
   */
  CompletionStage<Void> publish(DispatchEvent event, String eventType);
}
