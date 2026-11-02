package com.rcbill.notification.application.port;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

/** One bounded HTTP attempt. Never retries or follows redirects automatically. */
public interface HttpTransport {
    Result send(PreparedRequest request);

    /** Secret-bearing transient object: must not be logged, persisted or included in traces. */
    interface PreparedRequest {
        URI endpoint();
        String method();
        Map<String, String> headers();
        byte[] body();
        Duration totalTimeout();
        int maximumResponseBytes();
    }

    /** Only bounded response material; classification remains a separate policy. */
    interface Result {
        Integer httpStatus();
        String retryAfter();
        String transportErrorCode();
        boolean outcomeUnknown();
        byte[] boundedBody();
        long elapsedMs();
        String providerRequestId();
    }
}
