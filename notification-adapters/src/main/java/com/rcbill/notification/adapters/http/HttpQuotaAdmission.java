package com.rcbill.notification.adapters.http;
import com.rcbill.notification.application.port.*;
import tools.jackson.databind.json.JsonMapper;
import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Client of the external atomic rate/concurrency-permit service; never a local-only global quota. */
public final class HttpQuotaAdmission implements DispatchAdmission {
    private final HttpTransport transport;private final URI endpoint;private final SecretProvider secrets;private final String credentialRef;
    private final JsonMapper json;private final Clock clock;
    public HttpQuotaAdmission(HttpTransport transport,URI endpoint,SecretProvider secrets,String credentialRef,JsonMapper json,Clock clock){
        this.transport=transport;this.endpoint=endpoint;this.secrets=secrets;this.credentialRef=credentialRef;this.json=json;this.clock=clock;
    }
    private HttpTransport.Result call(URI uri,Map<String,Object> value){
        var headers=new HashMap<String,String>();headers.put("Content-Type","application/json");
        if(credentialRef!=null)try(var secret=secrets.resolve(credentialRef)){headers.put("Authorization","Bearer "+new String(secret.value()));}
        byte[] body=json.writeValueAsBytes(value);
        return transport.send(new HttpTransport.PreparedRequest(){
            public URI endpoint(){return uri;}public String method(){return "POST";}public Map<String,String> headers(){return headers;}
            public byte[] body(){return body;}public Duration totalTimeout(){return Duration.ofSeconds(2);}public int maximumResponseBytes(){return 8192;}
        });
    }
    @Override public Optional<Permit> tryAcquire(String provider){
        var response=call(endpoint.resolve("/v1/permits"),Map.of("providerBudgetKey",provider,"requestId",UUID.randomUUID().toString(),"leaseSeconds",30));
        if(!Integer.valueOf(200).equals(response.httpStatus())||response.transportErrorCode()!=null)return Optional.empty();
        try{
            var node=json.readTree(response.boundedBody());UUID id=UUID.fromString(node.get("permitId").stringValue());Instant expires=Instant.parse(node.get("expiresAt").stringValue());
            if(!expires.isAfter(clock.instant().plusSeconds(12))||expires.isAfter(clock.instant().plusSeconds(60)))return Optional.empty();
            return Optional.of(new Permit(){
                final AtomicBoolean released=new AtomicBoolean();public Instant validUntil(){return expires;}
                public void close(){if(released.compareAndSet(false,true))try{call(endpoint.resolve("/v1/permits/"+id+"/release"),Map.of());}catch(RuntimeException ignored){/* External lease expiry is the fallback. */}}
            });
        }catch(RuntimeException malformed){return Optional.empty();}
    }
}
