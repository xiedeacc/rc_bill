package com.rcbill.notification.worker;
import com.rcbill.notification.application.ServiceFailure;
import com.rcbill.notification.domain.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class KafkaReceiver {
    private final DeliveryDispatcher dispatcher;private final JsonMapper json;private final KafkaTemplate<String,String> template;
    private final String quarantine;private final MeterRegistry meters;
    public KafkaReceiver(DeliveryDispatcher dispatcher,JsonMapper json,KafkaTemplate<String,String> template,String quarantine,MeterRegistry meters){this.dispatcher=dispatcher;this.json=json;this.template=template;this.quarantine=quarantine;this.meters=meters;}
    public void receive(ConsumerRecord<String,String> record){
        DispatchEvent event;
        try{
            if(record.value()==null||record.value().getBytes(java.nio.charset.StandardCharsets.UTF_8).length>8192)throw new IllegalArgumentException();
            event=json.readValue(record.value(),DispatchEvent.class);
            if(event.schemaVersion()!=1||event.eventId()==null||event.notificationId()==null||event.destinationId()==null||event.destinationVersion()<1||event.createdAt()==null||event.expiresAt()==null||event.logicalSlot()!=Routing.slot(event.notificationId())||event.scheduleEpoch()<0||!event.notificationId().toString().equals(record.key()))throw new IllegalArgumentException();
        }catch(RuntimeException invalid){quarantine(record,"INVALID_EVENT");return;}
        try{dispatcher.handle(event);}catch(ServiceFailure failure){if(failure.status()==400)quarantine(record,failure.code());else throw failure;}
    }
    private void quarantine(ConsumerRecord<String,String> record,String code){
        String key=record.topic()+":"+record.partition()+":"+record.offset();
        String digest=java.util.HexFormat.of().formatHex(Routing.digest(record.value()==null?"":record.value()));
        String value=json.writeValueAsString(Map.of("sourceTopic",record.topic(),"sourcePartition",record.partition(),"sourceOffset",record.offset(),"code",code,"sha256",digest));
        try{template.send(quarantine,key,value).get(5,TimeUnit.SECONDS);meters.counter("notification.quarantine").increment();}
        catch(Exception failure){throw new IllegalStateException("Quarantine not durably confirmed",failure);}
    }
}
