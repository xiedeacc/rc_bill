package com.rcbill.notification.worker;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.*;
class AdministrativeJobsIT {
    @Test void migrationAndBootstrapAreExplicitAndPreserveExistingOffsets()throws Exception{
        String base=Objects.requireNonNull(System.getenv("TEST_PG_URL")),user=Objects.requireNonNull(System.getenv("TEST_PG_USER")),password=Objects.requireNonNull(System.getenv("TEST_PG_PASSWORD")),brokers=Objects.requireNonNull(System.getenv("TEST_KAFKA_BOOTSTRAP"));
        String id=UUID.randomUUID().toString().replace("-",""),schema="jobs_"+id,topic="jobs.dispatch."+id,dead="jobs.dead."+id,quarantine="jobs.quarantine."+id,group="jobs-group-"+id;
        try(var c=DriverManager.getConnection(base,user,password);var s=c.createStatement()){s.execute("CREATE SCHEMA "+schema);}
        var settings=new HashMap<String,String>();settings.putAll(Map.of("PG_URL",base+(base.contains("?")?"&":"?")+"currentSchema="+schema,"PG_USER",user,"PG_PASSWORD",password,"KAFKA_PARTITIONS","4","KAFKA_BOOTSTRAP_SERVERS",brokers,"KAFKA_REPLICATION",System.getenv().getOrDefault("TEST_KAFKA_REPLICATION","3"),"NOTIFICATION_KAFKA_DISPATCH_TOPIC",topic,"NOTIFICATION_KAFKA_DEAD_TOPIC",dead,"NOTIFICATION_KAFKA_QUARANTINE_TOPIC",quarantine,"NOTIFICATION_KAFKA_GROUP",group));
        try(var admin=Admin.create(Map.of("bootstrap.servers",brokers))){
            run("MigrationApplication",settings);run("MigrationApplication",settings);
            run("BootstrapApplication",settings);
            var partition=new TopicPartition(topic,0);admin.alterConsumerGroupOffsets(group,Map.of(partition,new OffsetAndMetadata(3))).all().get(10,TimeUnit.SECONDS);
            run("BootstrapApplication",settings);
            assertThat(admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10,TimeUnit.SECONDS).get(partition).offset()).isEqualTo(3);
            admin.deleteTopics(List.of(topic,dead,quarantine)).all().get(10,TimeUnit.SECONDS);
        }finally{try(var c=DriverManager.getConnection(base,user,password);var s=c.createStatement()){s.execute("DROP SCHEMA "+schema+" CASCADE");}}
    }
    void run(String main,Map<String,String> settings)throws Exception{
        Path java=Path.of(System.getProperty("java.home"),"bin",System.getProperty("os.name").startsWith("Windows")?"java.exe":"java");
        var builder=new ProcessBuilder(java.toString(),"-Dloader.main=com.rcbill.notification.worker."+main,"-cp",Path.of("target/notification-worker-0.1.0-SNAPSHOT.jar").toAbsolutePath().toString(),"org.springframework.boot.loader.launch.PropertiesLauncher");
        builder.environment().putAll(settings);Path log=Path.of("target/"+main+".log");var process=builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try{assertThat(process.waitFor(45,TimeUnit.SECONDS)).as(main+" must complete").isTrue();assertThat(process.exitValue()).as(Files.readString(log)).isZero();}finally{if(process.isAlive())process.destroyForcibly();}
    }
}
