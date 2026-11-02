package com.rcbill.notification.api;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import org.springframework.web.filter.OncePerRequestFilter;
import java.io.*;
import java.nio.charset.StandardCharsets;
public final class EnvelopeLimitFilter extends OncePerRequestFilter {
    private final java.util.concurrent.Semaphore capacity;
    public EnvelopeLimitFilter() {this(512);}
    public EnvelopeLimitFilter(int maximum) {if(maximum<1)throw new IllegalArgumentException("maximum");capacity=new java.util.concurrent.Semaphore(maximum);}
    @Override protected void doFilterInternal(HttpServletRequest request,HttpServletResponse response,FilterChain chain)throws ServletException,IOException{
        if(!request.getRequestURI().startsWith("/v1/notifications")){chain.doFilter(request,response);return;}
        if(!capacity.tryAcquire()){response.setHeader("Retry-After","1");ApiProblems.write(request,response,503,"API_CAPACITY");return;}
        try{bounded(request,response,chain);}finally{capacity.release();}
    }
    private void bounded(HttpServletRequest request,HttpServletResponse response,FilterChain chain)throws ServletException,IOException{
        if(!request.getMethod().equals("POST")||!request.getRequestURI().startsWith("/v1/notifications")){chain.doFilter(request,response);return;}
        String encoding=request.getHeader("Content-Encoding");
        if(encoding!=null&&!encoding.equalsIgnoreCase("identity")){ApiProblems.write(request,response,415,"UNSUPPORTED_ENCODING");return;}
        if(request.getContentLengthLong()>131072){ApiProblems.write(request,response,413,"ENVELOPE_TOO_LARGE");return;}
        byte[] bytes=request.getInputStream().readNBytes(131073);
        if(bytes.length>131072){ApiProblems.write(request,response,413,"ENVELOPE_TOO_LARGE");return;}
        var wrapper=new HttpServletRequestWrapper(request){
            public int getContentLength(){return bytes.length;}public long getContentLengthLong(){return bytes.length;}
            public ServletInputStream getInputStream(){var in=new ByteArrayInputStream(bytes);return new ServletInputStream(){
                public int read(){return in.read();}public boolean isFinished(){return in.available()==0;}public boolean isReady(){return true;}
                public void setReadListener(ReadListener listener){throw new UnsupportedOperationException("Blocking MVC request");}
            };}
            public BufferedReader getReader(){return new BufferedReader(new InputStreamReader(getInputStream(),StandardCharsets.UTF_8));}
        };chain.doFilter(wrapper,response);
    }
}
