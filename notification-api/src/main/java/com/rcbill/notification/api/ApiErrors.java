package com.rcbill.notification.api;
import com.rcbill.notification.application.ServiceFailure;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.context.annotation.Profile;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.net.URI;
import java.util.UUID;
@RestControllerAdvice @Profile("runtime")
public class ApiErrors {
    @ExceptionHandler(ServiceFailure.class) ResponseEntity<ProblemDetail> business(ServiceFailure error,HttpServletRequest request){return problem(error.status(),error.code(),request);}
    @ExceptionHandler({org.springframework.web.bind.MissingRequestHeaderException.class,org.springframework.web.method.annotation.MethodArgumentTypeMismatchException.class,org.springframework.http.converter.HttpMessageNotReadableException.class})
    ResponseEntity<ProblemDetail> malformed(Exception error,HttpServletRequest request){return problem(400,"INVALID_REQUEST",request);}
    @ExceptionHandler(org.springframework.web.HttpMediaTypeNotSupportedException.class) ResponseEntity<ProblemDetail> media(Exception error,HttpServletRequest request){return problem(415,"UNSUPPORTED_MEDIA_TYPE",request);}
    @ExceptionHandler(org.springframework.web.HttpRequestMethodNotSupportedException.class) ResponseEntity<ProblemDetail> method(Exception error,HttpServletRequest request){return problem(405,"METHOD_NOT_ALLOWED",request);}
    @ExceptionHandler(Exception.class) ResponseEntity<ProblemDetail> failure(Exception error,HttpServletRequest request){return problem(503,"TEMPORARILY_UNAVAILABLE",request);}
    static ResponseEntity<ProblemDetail> problem(int status,String code,HttpServletRequest request){
        var body=ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(status),code);body.setTitle(code);body.setType(URI.create("urn:rcbill:problem:"+code.toLowerCase(java.util.Locale.ROOT)));body.setInstance(URI.create(request.getRequestURI()));body.setProperty("code",code);body.setProperty("traceId",UUID.randomUUID().toString());
        var response=ResponseEntity.status(status).contentType(MediaType.APPLICATION_PROBLEM_JSON);if(status==429||status==503)response.header("Retry-After","5");return response.body(body);
    }
}
