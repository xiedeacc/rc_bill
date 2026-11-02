package com.rcbill.notification.application.port;

import com.rcbill.notification.application.NotificationUseCases;
import java.net.URI;
import java.util.Map;
import java.util.Set;

/** Trusted configuration only; callers cannot supply URLs or secret references. */
public interface DestinationCatalog {
    Snapshot resolveAuthorized(NotificationUseCases.Caller caller, String id, int version);

    /** Implementations must be immutable and redact sensitive data from diagnostic strings. */
    interface Snapshot {
        String id();
        int version();
        URI endpoint();
        String method();
        Set<String> contentTypes();
        Set<String> allowedDynamicHeaders();
        Map<String, String> staticHeaders();
        String credentialRef();
        String idempotencyHeader();
        DurationPolicy policy();
    }

    record DurationPolicy(int timeoutSeconds, int maxAttempts, long idempotencyWindowSeconds) { }
}
