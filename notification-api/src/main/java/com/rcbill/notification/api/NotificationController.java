package com.rcbill.notification.api;

import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** Route skeleton only. Validation and application use cases are intentionally not wired. */
@RestController
@org.springframework.context.annotation.Profile("scaffold")
@RequestMapping("/v1/notifications")
public class NotificationController {
    @PostMapping
    public ResponseEntity<?> submit(HttpServletRequest request) { return unavailable(request); }

    @GetMapping("/{notificationId}")
    public ResponseEntity<?> get(HttpServletRequest request) { return unavailable(request); }

    @GetMapping("/{notificationId}/attempts")
    public ResponseEntity<?> attempts(HttpServletRequest request) { return unavailable(request); }

    @PostMapping("/{notificationId}/replays")
    public ResponseEntity<?> replay(HttpServletRequest request) { return unavailable(request); }

    private ResponseEntity<?> unavailable(HttpServletRequest request) {
        var problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_IMPLEMENTED,
                "Scaffold only: durable acceptance and delivery are not implemented.");
        problem.setType(URI.create("urn:rcbill:problem:not-implemented"));
        problem.setTitle("Notification capability is not implemented");
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("code", "NOT_IMPLEMENTED");
        problem.setProperty("traceId", UUID.randomUUID().toString());
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
                .contentType(MediaType.APPLICATION_PROBLEM_JSON).body(problem);
    }
}
