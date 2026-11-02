package com.rcbill.notification.worker;

import com.rcbill.notification.adapters.config.*;
import com.rcbill.notification.adapters.http.*;
import com.rcbill.notification.adapters.security.*;
import com.rcbill.notification.adapters.persistence.*;
import com.rcbill.notification.adapters.kafka.KafkaDispatchPublisher;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.boot.health.contributor.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.config.ConfigResource;
import tools.jackson.databind.json.JsonMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Configuration(proxyBeanMethods=false) @Profile("runtime") @EnableScheduling
public class WorkerRuntimeConfiguration {
    private static String topic(Environment e,String kind){return e.getProperty("notification.kafka."+kind+"-topic","notification."+kind+".v1");}
    private static Map<String,Object> common(Environment e){
        var p=new HashMap<String,Object>();p.put("bootstrap.servers",e.getRequiredProperty("spring.kafka.bootstrap-servers"));
        for(String key:List.of("security.protocol","sasl.mechanism","sasl.jaas.config","ssl.truststore.location","ssl.truststore.password")){
            String value=e.getProperty("spring.kafka.properties."+key);if(value!=null)p.put(key,value);
        }
        p.put("ssl.endpoint.identification.algorithm","https");return p;
    }
    @Bean(destroyMethod="close") Admin kafkaAdminClient(Environment e,Shards shards)throws Exception{
        var admin=Admin.create(common(e));int replicas=e.getProperty("notification.kafka.required-replicas",Integer.class,3);
        try{
            var expected=Map.of(topic(e,"dispatch"),shards.partitions(),topic(e,"dead"),Math.min(48,shards.partitions()),topic(e,"quarantine"),Math.min(12,shards.partitions()));
            var actual=admin.describeTopics(expected.keySet()).allTopicNames().get(15,TimeUnit.SECONDS);
            for(var entry:expected.entrySet()){
                var description=actual.get(entry.getKey());
                if(description.partitions().size()!=entry.getValue()||description.partitions().stream().anyMatch(p->p.replicas().size()<replicas))throw new IllegalStateException("Kafka topology does not match configured durability");
                var resource=new ConfigResource(ConfigResource.Type.TOPIC,entry.getKey());var config=admin.describeConfigs(List.of(resource)).all().get(5,TimeUnit.SECONDS).get(resource);
                if(Integer.parseInt(config.get("min.insync.replicas").value())<Math.min(2,replicas)||!config.get("cleanup.policy").value().equals("delete"))throw new IllegalStateException("Unsafe topic configuration");
            }return admin;
        }catch(Exception error){admin.close();throw error;}
    }
    @Bean DefaultKafkaProducerFactory<String,String> notificationProducerFactory(Environment e){
        var p=common(e);p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class);p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class);
        p.put("acks","all");p.put("enable.idempotence",true);p.put("retries",Integer.MAX_VALUE);p.put("max.in.flight.requests.per.connection",5);
        p.put("delivery.timeout.ms",120000);p.put("request.timeout.ms",30000);p.put("max.block.ms",5000);p.put("linger.ms",5);p.put("batch.size",65536);p.put("buffer.memory",67108864L);p.put("compression.type","lz4");p.put("max.request.size",1048576);
        return new DefaultKafkaProducerFactory<>(p);
    }
    @Bean KafkaTemplate<String,String> notificationKafkaTemplate(DefaultKafkaProducerFactory<String,String> factory){return new KafkaTemplate<>(factory);}
    @Bean KafkaDispatchPublisher publisher(KafkaTemplate<String,String> template,JsonMapper json,Shards shards,Environment e){return new KafkaDispatchPublisher(template,json,shards.partitions(),topic(e,"dispatch"),topic(e,"dead"));}
    @Bean(destroyMethod="close") ApacheHttpTransport httpTransport(Environment e,ConfiguredCatalog catalog){
        var policy=new AddressPolicy(e.getProperty("notification.allow-loopback",Boolean.class,false));catalog.all().forEach(d->policy.validate(d.endpoint()));
        return new ApacheHttpTransport(policy,e.getProperty("notification.http.maximum",Integer.class,128));
    }
    @Bean EnvironmentSecrets providerSecrets(ConfiguredCatalog catalog,Environment e){var secrets=new EnvironmentSecrets(e::getProperty);for(var d:catalog.all())if(d.credentialRef()!=null)try(var ignored=secrets.resolve(d.credentialRef())){}return secrets;}
    @Bean HttpQuotaAdmission admission(ApacheHttpTransport transport,EnvironmentSecrets secrets,JsonMapper json,Environment e){
        URI uri=URI.create(e.getRequiredProperty("notification.quota.url"));new AddressPolicy(e.getProperty("notification.allow-loopback",Boolean.class,false)).validate(uri);
        String credential=e.getProperty("notification.quota.credential-ref");if(credential==null&&!e.getProperty("notification.allow-loopback",Boolean.class,false))throw new IllegalStateException("Quota service authentication required");
        return new HttpQuotaAdmission(transport,uri,secrets,credential,json,Clock.systemUTC());
    }
    @Bean(destroyMethod="close") DeliveryDispatcher dispatcher(PgDeliveryQueue queue,ApacheHttpTransport transport,EnvironmentSecrets secrets,HttpQuotaAdmission admission,ConfiguredCatalog catalog,Environment e,MeterRegistry meters){return new DeliveryDispatcher(queue,transport,secrets,admission,catalog,e.getProperty("notification.http.maximum",Integer.class,128),Clock.systemUTC(),meters);}
    @Bean WorkerJobs workerJobs(Shards shards,PgOutboxStore outbox,PgDeliveryQueue queue,KafkaDispatchPublisher publisher,Environment e,MeterRegistry meters){
        String role=e.getProperty("notification.role","all");if(!Set.of("all","relay","dispatch").contains(role))throw new IllegalArgumentException("Invalid worker role");
        return new WorkerJobs(shards,outbox,queue,publisher,role,meters);
    }
    @Bean ThreadPoolTaskScheduler taskScheduler(){var scheduler=new ThreadPoolTaskScheduler();scheduler.setPoolSize(3);scheduler.setThreadNamePrefix("control-");scheduler.setWaitForTasksToCompleteOnShutdown(true);scheduler.setAwaitTerminationSeconds(30);return scheduler;}
    @Bean ConcurrentMessageListenerContainer<String,String> dispatchContainer(Environment e,DeliveryDispatcher dispatcher,JsonMapper json,KafkaTemplate<String,String> template,MeterRegistry meters,Admin kafkaAdminClient){
        var p=common(e);p.put("key.deserializer",StringDeserializer.class);p.put("value.deserializer",StringDeserializer.class);p.put("group.id",e.getProperty("notification.kafka.group","notification-dispatch-v1"));
        p.put("enable.auto.commit",false);p.put("auto.offset.reset","none");p.put("isolation.level","read_committed");p.put("group.protocol","classic");p.put("partition.assignment.strategy","org.apache.kafka.clients.consumer.CooperativeStickyAssignor");p.put("max.poll.records",100);p.put("max.poll.interval.ms",300000);p.put("session.timeout.ms",45000);p.put("heartbeat.interval.ms",3000);p.put("allow.auto.create.topics",false);p.put("fetch.max.bytes",16777216);p.put("max.partition.fetch.bytes",1048576);
        var props=new ContainerProperties(topic(e,"dispatch"));props.setAckMode(ContainerProperties.AckMode.BATCH);props.setSyncCommits(true);props.setAsyncAcks(false);
        props.setListenerTaskExecutor(new SimpleAsyncTaskExecutor(Thread.ofPlatform().name("kafka-poll-",0).factory()));
        var receiver=new KafkaReceiver(dispatcher,json,template,topic(e,"quarantine"),meters);props.setMessageListener((MessageListener<String,String>)receiver::receive);
        var container=new ConcurrentMessageListenerContainer<>(new DefaultKafkaConsumerFactory<String,String>(p),props);
        container.setConcurrency(e.getProperty("notification.kafka.concurrency",Integer.class,2));container.setAutoStartup(!e.getProperty("notification.role","all").equals("relay"));
        var errors=new DefaultErrorHandler(new FixedBackOff(1000,FixedBackOff.UNLIMITED_ATTEMPTS));errors.setAckAfterHandle(false);errors.setCommitRecovered(false);errors.setClassifications(Map.of(),true);container.setCommonErrorHandler(errors);return container;
    }
    @Bean HealthIndicator workerHealthIndicator(WorkerJobs jobs,Admin admin){return ()->{try{admin.describeCluster().nodes().get(2,TimeUnit.SECONDS);return jobs.recovering()?Health.up().build():Health.down().withDetail("code","RECOVERY_NOT_READY").build();}catch(Exception failure){return Health.down().withDetail("code","KAFKA_UNAVAILABLE").build();}};}
    @Bean SecurityFilterChain workerSecurity(HttpSecurity http)throws Exception{return http.csrf(AbstractHttpConfigurer::disable).authorizeHttpRequests(a->a.requestMatchers("/actuator/health","/actuator/health/**").permitAll().anyRequest().denyAll()).build();}
}
