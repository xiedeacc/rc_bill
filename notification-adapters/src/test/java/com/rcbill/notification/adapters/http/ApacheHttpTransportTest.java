package com.rcbill.notification.adapters.http;
import com.rcbill.notification.application.port.HttpTransport;
import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.*;
class ApacheHttpTransportTest {
    HttpServer server;ApacheHttpTransport client;String base;
    @BeforeEach void start() throws Exception {server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());server.start();base="http://127.0.0.1:"+server.getAddress().getPort();client=new ApacheHttpTransport(new AddressPolicy(true),4);}
    @AfterEach void stop() throws Exception {client.close();server.stop(0);((ExecutorService)server.getExecutor()).close();}
    HttpTransport.PreparedRequest request(String path,Duration deadline){return new HttpTransport.PreparedRequest(){
        public URI endpoint(){return URI.create(base+path);}public String method(){return "POST";}
        public Map<String,String> headers(){return Map.of("Content-Type","text/plain","Idempotency-Key","stable");}
        public byte[] body(){return "中文".getBytes(java.nio.charset.StandardCharsets.UTF_8);}public Duration totalTimeout(){return deadline;}public int maximumResponseBytes(){return 65536;}
    };}
    @Test void sendsOriginalBytesAndDoesNotAutomaticallyRetry() {
        var count=new AtomicInteger();server.createContext("/failure",exchange->{count.incrementAndGet();assertThat(new String(exchange.getRequestBody().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("中文");exchange.getResponseHeaders().set("Retry-After","12");exchange.sendResponseHeaders(503,-1);exchange.close();});
        var result=client.send(request("/failure",Duration.ofSeconds(2)));assertThat(result.httpStatus()).isEqualTo(503);assertThat(result.retryAfter()).isEqualTo("12");assertThat(count.get()).isEqualTo(1);
    }
    @Test void redirectsNeverForwardAuthorizationOrRequest() {
        var count=new AtomicInteger();server.createContext("/redirect",e->{e.getResponseHeaders().set("Location",base+"/target");e.sendResponseHeaders(307,-1);e.close();});
        server.createContext("/target",e->{count.incrementAndGet();e.sendResponseHeaders(200,-1);e.close();});
        assertThat(client.send(request("/redirect",Duration.ofSeconds(2))).httpStatus()).isEqualTo(307);assertThat(count.get()).isZero();
    }
    @Test void boundsResponseAndActuallyCancelsStalledIo() throws Exception {
        server.createContext("/large",e->{byte[] data=new byte[70000];e.sendResponseHeaders(200,data.length);try{e.getResponseBody().write(data);}finally{e.close();}});
        assertThat(client.send(request("/large",Duration.ofSeconds(2))).transportErrorCode()).isEqualTo("RESPONSE_TOO_LARGE");
        var release=new CountDownLatch(1);server.createContext("/stall",e->{try{release.await();}catch(InterruptedException x){Thread.currentThread().interrupt();}finally{e.close();}});
        try{var r=client.send(request("/stall",Duration.ofMillis(200)));assertThat(r.outcomeUnknown()).isTrue();assertThat(r.elapsedMs()).isLessThan(2000);}finally{release.countDown();}
    }
    @Test void dnsLookupCannotOutliveTheRequestBudget()throws Exception{
        var release=new CountDownLatch(1);server.createContext("/dns",e->{e.sendResponseHeaders(204,-1);e.close();});
        client.close();client=new ApacheHttpTransport(new AddressPolicy(true),4,host->{try{release.await();return InetAddress.getAllByName(host);}catch(InterruptedException e){Thread.currentThread().interrupt();throw new UnknownHostException("cancelled test lookup");}});
        try{var result=client.send(request("/dns",Duration.ofMillis(200)));assertThat(result.outcomeUnknown()).isTrue();assertThat(result.elapsedMs()).isLessThan(1500);}finally{release.countDown();}
    }
}
