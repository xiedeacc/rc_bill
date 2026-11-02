package com.rcbill.notification.api;
import com.rcbill.notification.application.*;
import org.springframework.context.annotation.Profile;
import org.springframework.http.*;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;
import java.net.URI;
import java.util.*;
@RestController @Profile("runtime") @RequestMapping("/v1/notifications")
public class RuntimeNotificationController {
    private final NotificationService service;private final ApiJson json=new ApiJson();
    public RuntimeNotificationController(NotificationService service){this.service=service;}
    static NotificationUseCases.Caller caller(Jwt jwt){return new NotificationUseCases.Caller(jwt.getClaimAsString("tenant_id"),jwt.getClaimAsString("client_id"),jwt.getSubject());}
    private ResponseEntity<?> accepted(NotificationUseCases.Acceptance result){return ResponseEntity.accepted().location(URI.create("/v1/notifications/"+result.notificationId())).header("Idempotency-Replayed",Boolean.toString(result.replayed())).body(Map.of("notificationId",result.notificationId(),"acceptedAt",result.acceptedAt()));}
    @PostMapping(consumes=MediaType.APPLICATION_JSON_VALUE) public ResponseEntity<?> submit(@AuthenticationPrincipal Jwt jwt,@RequestHeader("Idempotency-Key") String key,@RequestBody byte[] body){return accepted(service.submit(caller(jwt),key,json.submission(body)));}
    @GetMapping("/{id}") public Object get(@AuthenticationPrincipal Jwt jwt,@PathVariable UUID id){return service.get(caller(jwt),id).orElseThrow(()->new ServiceFailure(404,"NOT_FOUND"));}
    @GetMapping("/{id}/attempts") public Object attempts(@AuthenticationPrincipal Jwt jwt,@PathVariable UUID id,@RequestParam(defaultValue="0") int afterAttempt,@RequestParam(defaultValue="20") int limit){return service.attempts(caller(jwt),id,afterAttempt,limit);}
    @PostMapping(path="/{id}/replays",consumes=MediaType.APPLICATION_JSON_VALUE) public ResponseEntity<?> replay(@AuthenticationPrincipal Jwt jwt,@PathVariable UUID id,@RequestHeader("Idempotency-Key") String key,@RequestBody byte[] body){return accepted(service.replay(caller(jwt),id,key,json.replay(body)));}
}
