package com.rcbill.notification.adapters.config;
import com.rcbill.notification.application.*;
import com.rcbill.notification.adapters.persistence.*;
import com.rcbill.notification.adapters.security.*;
import tools.jackson.databind.json.JsonMapper;
import org.springframework.context.annotation.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.core.env.Environment;
import org.springframework.boot.health.contributor.*;
import java.nio.file.Path;
import java.util.*;
@Configuration(proxyBeanMethods=false) @Profile("runtime")
public class PersistenceConfiguration {
    @Bean JsonMapper ledgerJson(){return JsonMapper.builder().disable(tools.jackson.databind.cfg.DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS).changeDefaultPropertyInclusion(v->v.withValueInclusion(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)).enable(tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).build();}
    @Bean(destroyMethod="close") Shards shards(Environment env){return new Shards(Arrays.asList(env.getRequiredProperty("notification.database.urls").split(",")),env.getRequiredProperty("notification.database.user"),env.getRequiredProperty("notification.database.password"),env.getProperty("notification.database.migrate",Boolean.class,false),env.getProperty("notification.kafka.partitions",Integer.class,16));}
    @Bean @ConditionalOnExpression("'${notification.role:all}' != 'relay'") PayloadCipher payloadCipher(Environment env){return new PayloadCipher(env.getRequiredProperty("notification.encryption-key"));}
    @Bean @ConditionalOnExpression("'${notification.role:all}' != 'relay'") ConfiguredCatalog catalog(Environment env,JsonMapper json)throws Exception{return ConfiguredCatalog.load(Path.of(env.getRequiredProperty("notification.destinations-file")),json);}
    @Bean @ConditionalOnExpression("'${notification.role:all}' != 'relay'") PgNotificationStore notificationStore(Shards shards,PayloadCipher cipher,JsonMapper json,ConfiguredCatalog catalog,Environment env){var store=new PgNotificationStore(shards,cipher,json,env.getProperty("notification.admission.maximum-pending",Integer.class,50000),java.time.Duration.ofSeconds(1));catalog.all().forEach(store::registerDestination);return store;}
    @Bean @ConditionalOnExpression("'${notification.role:all}' != 'relay'") PgDeliveryQueue deliveryQueue(Shards shards,PayloadCipher cipher,JsonMapper json){return new PgDeliveryQueue(shards,cipher,json);}
    @Bean PgOutboxStore outboxStore(Shards shards){return new PgOutboxStore(shards);}
    @Bean @ConditionalOnExpression("'${notification.role:all}' != 'relay'") NotificationService notificationService(PgNotificationStore store,ConfiguredCatalog catalog){return new NotificationService(store,catalog);}
    @Bean HealthIndicator ledgerHealthIndicator(Shards shards){return ()->{try{for(var shard:shards.all())shard.mapper().now();return Health.up().build();}catch(Exception e){return Health.down().withDetail("code","LEDGER_UNAVAILABLE").build();}};}
}
