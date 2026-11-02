package com.rcbill.notification.adapters.persistence;
import com.rcbill.notification.application.*;
import com.rcbill.notification.application.port.DestinationCatalog;
import com.rcbill.notification.adapters.config.*;
import com.rcbill.notification.adapters.security.PayloadCipher;
import tools.jackson.databind.json.JsonMapper;
import java.net.URI;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
class ShardingIT {
    @Test void requestsRouteAcrossDatasourcesAndReorderingFailsClosed()throws Exception{
        String base=Objects.requireNonNull(System.getenv("TEST_PG_URL")),user=Objects.requireNonNull(System.getenv("TEST_PG_USER")),password=Objects.requireNonNull(System.getenv("TEST_PG_PASSWORD"));
        String prefix="shards_"+UUID.randomUUID().toString().replace("-","");var schemas=List.of(prefix+"a",prefix+"b");
        try(var c=DriverManager.getConnection(base,user,password);var s=c.createStatement()){for(var schema:schemas)s.execute("CREATE SCHEMA "+schema);}
        var urls=schemas.stream().map(schema->base+(base.contains("?")?"&":"?")+"currentSchema="+schema).toList();
        try(var shards=new Shards(urls,user,password,true,16)){
            var store=new PgNotificationStore(shards,new PayloadCipher(Base64.getEncoder().encodeToString(new byte[32])),JsonMapper.builder().build());
            var d=new Destination("crm-test",1,Set.of("t"),Set.of("c"),true,URI.create("https://example.com/notify"),"POST",Set.of("text/plain"),Set.of(),Map.of(),null,"Idempotency-Key",new DestinationCatalog.DurationPolicy(2,20,86400));
            var service=new NotificationService(store,new ConfiguredCatalog(List.of(d)));var caller=new NotificationUseCases.Caller("t","c","s");var request=new SubmissionRequest("crm-test",1,"text/plain","body",Map.of(),60);
            var owners=new HashSet<Integer>();
            for(int i=0;i<20;i++){var a=service.submit(caller,"sharded-key-"+i,request);owners.add(com.rcbill.notification.domain.Routing.slot(a.notificationId())%2);assertThat(service.get(caller,a.notificationId())).isPresent();assertThat(service.submit(caller,"sharded-key-"+i,request).notificationId()).isEqualTo(a.notificationId());}
            assertThat(owners).containsExactlyInAnyOrder(0,1);
            assertThatThrownBy(()->new Shards(List.of(urls.get(1),urls.get(0)),user,password,false,16)).isInstanceOf(IllegalStateException.class);
            try(var c=DriverManager.getConnection(base,user,password);var s=c.createStatement()){
                s.execute("ALTER TABLE "+schemas.get(0)+".notification RENAME TO temporarily_unavailable");
                try{var queue=new PgDeliveryQueue(shards,new PayloadCipher(Base64.getEncoder().encodeToString(new byte[32])),JsonMapper.builder().build());assertThatCode(()->queue.expireQueued(100)).doesNotThrowAnyException();}
                finally{s.execute("ALTER TABLE "+schemas.get(0)+".temporarily_unavailable RENAME TO notification");}
            }
        }finally{try(var c=DriverManager.getConnection(base,user,password);var s=c.createStatement()){for(var schema:schemas)s.execute("DROP SCHEMA "+schema+" CASCADE");}}
    }
}
