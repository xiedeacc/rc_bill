package com.rcbill.notification.adapters.http;
import com.rcbill.notification.application.port.HttpTransport;
import java.net.*;
import java.util.concurrent.*;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.*;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.util.Timeout;
public final class ApacheHttpTransport implements HttpTransport,AutoCloseable {
    @FunctionalInterface public interface Lookup {InetAddress[] resolve(String host)throws UnknownHostException;}
    private final AddressPolicy policy;private final CloseableHttpClient client;
    private final ThreadLocal<Long> requestDeadline=new ThreadLocal<>();
    private final ThreadPoolExecutor dns=new ThreadPoolExecutor(4,4,30,TimeUnit.SECONDS,new ArrayBlockingQueue<>(64),Thread.ofPlatform().daemon(true).name("dns-",0).factory(),new ThreadPoolExecutor.AbortPolicy());
    private final ScheduledExecutorService canceller=Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon(true).name("http-deadline").factory());
    public ApacheHttpTransport(AddressPolicy policy,int connections) {this(policy,connections,InetAddress::getAllByName);}
    public ApacheHttpTransport(AddressPolicy policy,int connections,Lookup lookup) {
        this.policy=policy;
        var pool=PoolingHttpClientConnectionManagerBuilder.create().setMaxConnTotal(connections).setMaxConnPerRoute(connections)
                .setDnsResolver(new DnsResolver(){
                    public InetAddress[] resolve(String host)throws UnknownHostException{
                        Future<InetAddress[]> future=null;
                        try{
                            long remaining=Math.min(TimeUnit.SECONDS.toNanos(2),requestDeadline.get()-System.nanoTime());
                            if(remaining<=0)throw new TimeoutException();
                            future=dns.submit(()->lookup.resolve(host));return policy.validateAddresses(future.get(remaining,TimeUnit.NANOSECONDS));
                        }catch(InterruptedException e){Thread.currentThread().interrupt();throw new UnknownHostException("DNS interrupted");}
                        catch(ExecutionException|TimeoutException|RejectedExecutionException e){throw new UnknownHostException("DNS deadline or capacity exceeded");}
                        finally{if(future!=null&&!future.isDone())future.cancel(true);dns.purge();}
                    }
                    public String resolveCanonicalHostname(String host){return host;}
                }).build();
        client=HttpClients.custom().setConnectionManager(pool).disableAutomaticRetries().disableRedirectHandling()
                .disableCookieManagement().disableContentCompression().setUserAgent("rc-bill/1").build();
    }
    private record Response(Integer httpStatus,String retryAfter,String transportErrorCode,boolean outcomeUnknown,byte[] boundedBody,long elapsedMs,String providerRequestId) implements Result {
        @Override public String toString(){return "HttpResult[status="+httpStatus+", code="+transportErrorCode+"]";}
    }
    public Result send(PreparedRequest input){
        long start=System.nanoTime();
        try{policy.validate(input.endpoint());}catch(IllegalArgumentException invalid){return new Response(null,null,"BLOCKED_ADDRESS",false,new byte[0],0,null);}
        var request=new HttpUriRequestBase(input.method(),input.endpoint());
        request.setConfig(RequestConfig.custom().setConnectionRequestTimeout(Timeout.ofSeconds(2)).setResponseTimeout(Timeout.ofMilliseconds(input.totalTimeout().toMillis())).build());
        input.headers().forEach(request::setHeader);
        request.setEntity(new ByteArrayEntity(input.body(),ContentType.APPLICATION_OCTET_STREAM));
        var cancellation=canceller.schedule(request::cancel,input.totalTimeout().toMillis(),TimeUnit.MILLISECONDS);
        requestDeadline.set(System.nanoTime()+input.totalTimeout().toNanos());
        try(var response=client.executeOpen(null,request,null)){
            int status=response.getCode();String after=response.getFirstHeader("Retry-After")==null?null:response.getFirstHeader("Retry-After").getValue();
            String provider=response.getFirstHeader("X-Request-ID")==null?null:response.getFirstHeader("X-Request-ID").getValue();
            if(provider!=null&&(provider.length()>128||provider.chars().anyMatch(c->c<32)))provider=null;
            byte[] body=response.getEntity()==null?new byte[0]:response.getEntity().getContent().readNBytes(Math.min(65536,input.maximumResponseBytes())+1);
            if(body.length>input.maximumResponseBytes()){request.cancel();return new Response(status,after,"RESPONSE_TOO_LARGE",false,new byte[0],elapsed(start),provider);}
            return new Response(status,after,null,false,body,elapsed(start),provider);
        }catch(Exception failure){
            boolean tls=false;for(Throwable t=failure;t!=null;t=t.getCause())if(t instanceof javax.net.ssl.SSLException)tls=true;
            request.cancel();return new Response(null,null,tls?"TLS_ERROR":"IO_UNKNOWN",!tls,new byte[0],elapsed(start),null);
        }finally{cancellation.cancel(false);requestDeadline.remove();}
    }
    private static long elapsed(long start){return TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-start);}
    public void close(){canceller.shutdownNow();dns.shutdownNow();try{client.close();}catch(java.io.IOException ignored){}}
}
