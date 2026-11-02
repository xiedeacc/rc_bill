package com.rcbill.notification.api;
import org.springframework.mock.web.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
class EnvelopeLimitFilterTest {
    MockHttpServletRequest request(){var r=new MockHttpServletRequest("POST","/v1/notifications");r.setContent("{}".getBytes());return r;}
    @Test void activeRequestsAreBoundedBeforeAllocatingAnotherEnvelope()throws Exception{
        var filter=new EnvelopeLimitFilter(1);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var executor=Executors.newVirtualThreadPerTaskExecutor()){
            var first=executor.submit(()->{filter.doFilter(request(),new MockHttpServletResponse(),(r,s)->{entered.countDown();try{release.await();}catch(InterruptedException e){Thread.currentThread().interrupt();}});return null;});
            try{assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();var second=new MockHttpServletResponse();filter.doFilter(request(),second,(r,s)->{});assertThat(second.getStatus()).isEqualTo(503);}finally{release.countDown();}
            first.get(5,TimeUnit.SECONDS);var third=new MockHttpServletResponse();filter.doFilter(request(),third,(r,s)->s.getWriter().write("ok"));assertThat(third.getContentAsString()).isEqualTo("ok");
        }finally{release.countDown();}
    }
    @Test void oversizedAndCompressedBodiesNeverReachController()throws Exception{
        var filter=new EnvelopeLimitFilter();var big=request();big.setContent(new byte[131073]);var response=new MockHttpServletResponse();
        filter.doFilter(big,response,(r,s)->{throw new AssertionError("controller must not run");});assertThat(response.getStatus()).isEqualTo(413);
        var compressed=request();compressed.addHeader("Content-Encoding","gzip");response=new MockHttpServletResponse();filter.doFilter(compressed,response,(r,s)->{throw new AssertionError("controller must not run");});assertThat(response.getStatus()).isEqualTo(415);
    }
}
