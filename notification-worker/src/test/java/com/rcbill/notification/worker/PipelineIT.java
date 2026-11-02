package com.rcbill.notification.worker;

import com.rcbill.notification.application.*;
import com.rcbill.notification.application.port.*;
import com.rcbill.notification.adapters.config.*;
import com.rcbill.notification.adapters.persistence.*;
import com.rcbill.notification.adapters.security.*;
import com.rcbill.notification.adapters.http.*;
import com.rcbill.notification.adapters.kafka.*;
import com.rcbill.notification.domain.*;
import tools.jackson.databind.json.JsonMapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.security.*;
import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/** Full HTTP API -> real PG -> real Kafka -> Worker -> external-service fixtures. */
class PipelineIT {
    final JsonMapper json=JsonMapper.builder().build();
    final HttpClient http=HttpClient.newHttpClient();
    static String env(String key){return Objects.requireNonNull(System.getenv(key),key+" required; do not silently skip integration");}
    @Test void deliveryRetryDuplicateAndDurableHandoffRecovery()throws Exception{
        String base=env("TEST_PG_URL"),user=env("TEST_PG_USER"),password=env("TEST_PG_PASSWORD"),bootstrap=env("TEST_KAFKA_BOOTSTRAP");
        String suffix=UUID.randomUUID().toString().replace("-",""),schema="pipeline_"+suffix,dispatch="it.dispatch."+suffix,dead="it.dead."+suffix,quarantine="it.quarantine."+suffix,group="it-group-"+suffix;
        int replicas=Integer.parseInt(System.getenv().getOrDefault("TEST_KAFKA_REPLICATION","3"));
        try(var connection=DriverManager.getConnection(base,user,password);var stmt=connection.createStatement()){stmt.execute("CREATE SCHEMA "+schema);}
        String url=base+(base.contains("?")?"&":"?")+"currentSchema="+schema;
        String encryption=Base64.getEncoder().encodeToString(new byte[32]);
        HttpServer external=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);var externalPool=Executors.newVirtualThreadPerTaskExecutor();external.setExecutor(externalPool);
        var calls=new AtomicInteger();var keys=ConcurrentHashMap.<String>newKeySet();var first=new AtomicBoolean(true);
        external.createContext("/notify",exchange->{calls.incrementAndGet();keys.add(exchange.getRequestHeaders().getFirst("Idempotency-Key"));exchange.getRequestBody().readAllBytes();exchange.getResponseHeaders().set("Retry-After","0");exchange.sendResponseHeaders(first.getAndSet(false)?503:204,-1);exchange.close();});
        external.createContext("/v1/permits",exchange->{exchange.getRequestBody().readAllBytes();byte[] bytes=json.writeValueAsBytes(exchange.getRequestURI().getPath().endsWith("/release")?Map.of():Map.of("permitId",UUID.randomUUID().toString(),"expiresAt",Instant.now().plusSeconds(30).toString()));exchange.getResponseHeaders().set("Content-Type","application/json");exchange.sendResponseHeaders(200,bytes.length);exchange.getResponseBody().write(bytes);exchange.close();});external.start();
        Path temp=Files.createTempDirectory("notification-pipeline-");Process api=null;
        var d=new Destination("crm-test",1,Set.of("tenant-a"),Set.of("client-a"),true,URI.create("http://127.0.0.1:"+external.getAddress().getPort()+"/notify"),"POST",Set.of("text/plain"),Set.of(),Map.of(),null,"Idempotency-Key",new DestinationCatalog.DurationPolicy(2,20,2592000));
        var catalog=new ConfiguredCatalog(List.of(d));var cipher=new PayloadCipher(encryption);var meters=new SimpleMeterRegistry();
        try(var shards=new Shards(List.of(url),user,password,true,16);var admin=Admin.create(Map.of("bootstrap.servers",bootstrap));var transport=new ApacheHttpTransport(new AddressPolicy(true),16)){
            admin.createTopics(List.of(new NewTopic(dispatch,16,(short)replicas),new NewTopic(dead,16,(short)replicas),new NewTopic(quarantine,12,(short)replicas)).stream().peek(t->t.configs(Map.of("min.insync.replicas",Integer.toString(Math.min(2,replicas)),"cleanup.policy","delete"))).toList()).all().get(30,TimeUnit.SECONDS);
            Map<TopicPartition,OffsetAndMetadata> offsets=new HashMap<>();for(int i=0;i<16;i++)offsets.put(new TopicPartition(dispatch,i),new OffsetAndMetadata(0));admin.alterConsumerGroupOffsets(group,offsets).all().get(15,TimeUnit.SECONDS);
            var store=new PgNotificationStore(shards,cipher,json);var queue=new PgDeliveryQueue(shards,cipher,json);var outbox=new PgOutboxStore(shards);var service=new NotificationService(store,catalog);
            var settings=new MockEnvironment().withProperty("spring.kafka.bootstrap-servers",bootstrap).withProperty("notification.kafka.dispatch-topic",dispatch).withProperty("notification.kafka.dead-topic",dead).withProperty("notification.kafka.quarantine-topic",quarantine).withProperty("notification.kafka.group",group).withProperty("notification.kafka.concurrency","2");
            var cfg=new WorkerRuntimeConfiguration();var factory=cfg.notificationProducerFactory(settings);var template=cfg.notificationKafkaTemplate(factory);var publisher=new KafkaDispatchPublisher(template,json,16,dispatch,dead);
            var secrets=new EnvironmentSecrets(k->null);var admission=new HttpQuotaAdmission(transport,URI.create("http://127.0.0.1:"+external.getAddress().getPort()),secrets,null,json,Clock.systemUTC());
            try(var dispatcher=new DeliveryDispatcher(queue,transport,secrets,admission,catalog,16,Clock.systemUTC(),meters)){
                var jobs=new WorkerJobs(shards,outbox,queue,publisher,"all",meters);var container=cfg.dispatchContainer(settings,dispatcher,json,template,meters,admin);
                var scheduler=Executors.newScheduledThreadPool(2);container.start();
                var publishing=scheduler.scheduleWithFixedDelay(jobs::publishDue,0,100,TimeUnit.MILLISECONDS);var recovery=scheduler.scheduleWithFixedDelay(jobs::recover,0,200,TimeUnit.MILLISECONDS);
                try{
                    KeyPairGenerator gen=KeyPairGenerator.getInstance("RSA");gen.initialize(2048);KeyPair pair=gen.generateKeyPair();
                    Path publicKey=temp.resolve("public.pem"),destinations=temp.resolve("destinations.json");Files.writeString(publicKey,"-----BEGIN PUBLIC KEY-----\n"+Base64.getEncoder().encodeToString(pair.getPublic().getEncoded())+"\n-----END PUBLIC KEY-----");Files.writeString(destinations,json.writeValueAsString(List.of(d)));
                    int port;try(var socket=new java.net.ServerSocket(0,0,InetAddress.getByName("127.0.0.1"))){port=socket.getLocalPort();}
                    Path java=Path.of(System.getProperty("java.home"),"bin",System.getProperty("os.name").startsWith("Windows")?"java.exe":"java");
                    var process=new ProcessBuilder(java.toString(),"-jar",Path.of("../notification-api/target/notification-api-0.1.0-SNAPSHOT.jar").toAbsolutePath().toString(),"--spring.profiles.active=runtime","--server.port="+port,"--notification.allow-loopback=true","--notification.security.public-key-file="+publicKey);
                    process.environment().putAll(Map.of("PG_URL",url,"PG_USER",user,"PG_PASSWORD",password,"DB_MIGRATE","false","NOTIFICATION_ENCRYPTION_KEY",encryption,"DESTINATIONS_FILE",destinations.toString(),"JWT_ISSUER","https://issuer.example","JWT_AUDIENCE","notifications"));
                    Path log=Path.of("target/pipeline-api.log");api=process.redirectErrorStream(true).redirectOutput(log.toFile()).start();final Process running=api;
                    await().atMost(Duration.ofSeconds(40)).ignoreExceptions().until(()->{if(!running.isAlive())throw new AssertionError(Files.readString(log));return http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+port+"/actuator/health/liveness")).GET().build(),HttpResponse.BodyHandlers.ofString()).statusCode()==200;});
                    String token=token(pair,"notifications:write notifications:read notifications:replay","notifications",Instant.now().plusSeconds(300));
                    String endpoint="http://127.0.0.1:"+port+"/v1/notifications";
                    String body="{\"destinationId\":\"crm-test\",\"destinationVersion\":1,\"contentType\":\"text/plain\",\"body\":\"hello\",\"ttlSeconds\":60}";
                    var accepted=post(endpoint,token,"pipeline-key",body);assertThat(accepted.statusCode()).as(accepted.body()).isEqualTo(202);
                    UUID task=UUID.fromString(json.readTree(accepted.body()).get("notificationId").stringValue());
                    assertThat(post(endpoint,token,"pipeline-key",body).body()).isEqualTo(accepted.body());
                    assertThat(post(endpoint,token,"pipeline-key",body.replace("hello","changed")).statusCode()).isEqualTo(409);
                    assertThat(post(endpoint,token(pair,"notifications:read","notifications",Instant.now().plusSeconds(300)),"other-key",body).statusCode()).isEqualTo(403);
                    assertThat(post(endpoint,token(pair,"notifications:write","wrong-audience",Instant.now().plusSeconds(300)),"other-key",body).statusCode()).isEqualTo(401);
                    var caller=new NotificationUseCases.Caller("tenant-a","client-a","operator");
                    await().atMost(Duration.ofSeconds(40)).until(()->service.get(caller,task).orElseThrow().status()==NotificationStatus.SUCCEEDED);
                    assertThat(calls.get()).isEqualTo(2);assertThat(keys).hasSize(1);
                    var duplicate=new DispatchEvent(1,UUID.randomUUID(),task,Routing.slot(task),0,"crm-test",Instant.now(),Instant.now().plusSeconds(60));publisher.publish(duplicate,"DISPATCH").toCompletableFuture().get(10,TimeUnit.SECONDS);
                    await().during(Duration.ofMillis(700)).atMost(Duration.ofSeconds(5)).untilAsserted(()->assertThat(calls.get()).isEqualTo(2));
                    // Controlled crash window: consume + durable claim + offset commit; intentionally discard HTTP execution.
                    publishing.cancel(false);recovery.cancel(false);container.stop();
                    var abandoned=service.submit(caller,"crash-window-key",new SubmissionRequest("crm-test",1,"text/plain","crash",Map.of(),60));
                    var publication=outbox.leaseDue(0,0,100,Duration.ofSeconds(180)).stream().filter(p->p.event().notificationId().equals(abandoned.notificationId())).findFirst().orElseThrow();
                    publisher.publish(publication.event(),"DISPATCH").toCompletableFuture().get(10,TimeUnit.SECONDS);outbox.markPublished(publication);
                    var consumerProps=new HashMap<String,Object>();consumerProps.put("bootstrap.servers",bootstrap);consumerProps.put("group.id",group);consumerProps.put("group.protocol","classic");consumerProps.put("partition.assignment.strategy","org.apache.kafka.clients.consumer.CooperativeStickyAssignor");consumerProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");consumerProps.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");consumerProps.put("enable.auto.commit",false);consumerProps.put("auto.offset.reset","none");
                    var held=new AtomicReference<DeliveryQueue.Lease>();
                    try(var consumer=new KafkaConsumer<String,String>(consumerProps)){
                        consumer.subscribe(List.of(dispatch));long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(20);
                        while(held.get()==null&&System.nanoTime()<end)for(var record:consumer.poll(Duration.ofMillis(200)))if(abandoned.notificationId().toString().equals(record.key())){
                            held.set(queue.claim(publication.event(),Duration.ofSeconds(60)).lease());consumer.commitSync(Map.of(new TopicPartition(record.topic(),record.partition()),new OffsetAndMetadata(record.offset()+1)));break;
                        }
                    }
                    assertThat(held.get()).isNotNull();
                    try(var c=DriverManager.getConnection(url,user,password);var p=c.prepareStatement("UPDATE notification SET lease_until=clock_timestamp()-interval '1 second' WHERE id=?")){p.setObject(1,abandoned.notificationId());p.executeUpdate();}
                    queue.recoverExpiredLeases(100);
                    assertThat(queue.complete(held.get(),new DeliveryQueue.Completion(NotificationStatus.SUCCEEDED,AttemptOutcome.SUCCEEDED,null,204,null,0,null))).isFalse();
                    container.start();scheduler.scheduleWithFixedDelay(jobs::publishDue,0,100,TimeUnit.MILLISECONDS);scheduler.scheduleWithFixedDelay(jobs::recover,0,200,TimeUnit.MILLISECONDS);
                    await().atMost(Duration.ofSeconds(40)).until(()->service.get(caller,abandoned.notificationId()).orElseThrow().status()==NotificationStatus.SUCCEEDED);
                    assertThat(service.attempts(caller,abandoned.notificationId(),0,20).items()).extracting(AttemptView::outcome).containsExactly(AttemptOutcome.UNKNOWN,AttemptOutcome.SUCCEEDED);
                    assertThat(calls.get()).isEqualTo(3);
                }finally{scheduler.shutdownNow();container.stop();scheduler.awaitTermination(10,TimeUnit.SECONDS);factory.destroy();}
            }
            admin.deleteTopics(List.of(dispatch,dead,quarantine)).all().get(15,TimeUnit.SECONDS);
        }finally{
            if(api!=null){api.destroy();if(!api.waitFor(10,TimeUnit.SECONDS))api.destroyForcibly();}
            external.stop(0);externalPool.close();meters.close();
            try(var c=DriverManager.getConnection(base,user,password);var s=c.createStatement()){s.execute("DROP SCHEMA "+schema+" CASCADE");}
        }
    }
    HttpResponse<String> post(String url,String token,String key,String body)throws Exception{return http.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).header("Authorization","Bearer "+token).header("Idempotency-Key",key).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build(),HttpResponse.BodyHandlers.ofString());}
    String token(KeyPair key,String scope,String audience,Instant expiry)throws Exception{
        var base64=Base64.getUrlEncoder().withoutPadding();String header=base64.encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String body=base64.encodeToString(json.writeValueAsBytes(Map.of("iss","https://issuer.example","aud",List.of(audience),"sub","operator","tenant_id","tenant-a","client_id","client-a","scope",scope,"exp",expiry.getEpochSecond())));
        var signer=Signature.getInstance("SHA256withRSA");signer.initSign(key.getPrivate());signer.update((header+"."+body).getBytes(java.nio.charset.StandardCharsets.US_ASCII));return header+"."+body+"."+base64.encodeToString(signer.sign());
    }
}
