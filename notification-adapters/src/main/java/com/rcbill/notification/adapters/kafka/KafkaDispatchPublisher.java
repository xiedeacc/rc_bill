package com.rcbill.notification.adapters.kafka;

import com.rcbill.notification.application.port.DispatchPublisher;
import com.rcbill.notification.domain.DispatchEvent;
import java.util.concurrent.CompletionStage;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

public final class KafkaDispatchPublisher implements DispatchPublisher {
  private final KafkaTemplate<String, String> template;
  private final JsonMapper json;
  private final int partitions;
  private final String dispatchTopic, deadTopic;

  public KafkaDispatchPublisher(
      KafkaTemplate<String, String> template,
      JsonMapper json,
      int partitions,
      String dispatchTopic,
      String deadTopic) {
    this.template = template;
    this.json = json;
    this.partitions = partitions;
    this.dispatchTopic = dispatchTopic;
    this.deadTopic = deadTopic;
  }

  @Override
  public CompletionStage<Void> publish(DispatchEvent event, String type) {
    if (!java.util.Set.of("DISPATCH", "DEAD").contains(type))
      throw new IllegalArgumentException("Unknown event channel");
    String topic = type.equals("DISPATCH") ? dispatchTopic : deadTopic;
    int count = type.equals("DISPATCH") ? partitions : Math.min(48, partitions);
    return template
        .send(
            topic,
            event.logicalSlot() % count,
            event.notificationId().toString(),
            json.writeValueAsString(event))
        .thenApply(result -> null);
  }
}
