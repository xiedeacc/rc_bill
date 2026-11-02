package com.rcbill.notification.application;

import java.util.Map;
import java.util.TreeMap;
import java.util.Locale;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import com.rcbill.notification.domain.Routing;

public record SubmissionRequest(String destinationId, int destinationVersion, String contentType,
                                String body, Map<String, String> headers, Integer ttlSecondsValue)
        implements NotificationUseCases.Submission {
    @Override public int ttlSeconds() { return ttlSecondsValue == null ? 86400 : ttlSecondsValue; }
    public SubmissionRequest validated() {
        if (destinationId == null || !destinationId.matches("[a-z][a-z0-9-]{2,63}") || destinationVersion < 1
                || contentType == null || !Set.of("application/json","application/x-www-form-urlencoded","text/plain").contains(contentType)
                || body == null || ttlSeconds() < 60 || ttlSeconds() > 86400) throw new ServiceFailure(400,"INVALID_REQUEST");
        if (bytes(body) > 65536) throw new ServiceFailure(413,"BODY_TOO_LARGE");
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(body)) throw new ServiceFailure(400,"INVALID_BODY_ENCODING");
        Map<String,String> normalized = new TreeMap<>();
        int total = 0;
        if (headers != null) for (var entry : headers.entrySet()) {
            String name = entry.getKey(), value = entry.getValue();
            if (name == null || !name.matches("[!#$%&'*+.^_`|~0-9A-Za-z-]{1,64}") || value == null
                    || value.length() > 1024 || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0
                    || value.chars().anyMatch(c -> (c < 32 && c != 9) || c > 126)) throw new ServiceFailure(400,"INVALID_HEADER");
            name = name.toLowerCase(Locale.ROOT);
            if (Set.of("host","authorization","cookie","content-length","transfer-encoding","connection",
                    "content-type","expect","upgrade","te","trailer","keep-alive").contains(name)
                    || name.startsWith("proxy-") || name.startsWith("x-forwarded-") || name.equals("forwarded")
                    || normalized.putIfAbsent(name,value) != null) throw new ServiceFailure(400,"INVALID_HEADER");
            total += bytes(name) + bytes(value);
        }
        if (normalized.size() > 16 || total > 8192) throw new ServiceFailure(413,"HEADERS_TOO_LARGE");
        return new SubmissionRequest(destinationId,destinationVersion,contentType,body,Map.copyOf(normalized),ttlSeconds());
    }
    public String fingerprint() {
        List<String> parts = new ArrayList<>(List.of(destinationId,Integer.toString(destinationVersion),contentType,body,Integer.toString(ttlSeconds())));
        new TreeMap<>(headers == null ? Map.<String,String>of() : headers).forEach((k,v) -> { parts.add(k); parts.add(v); });
        return java.util.HexFormat.of().formatHex(Routing.digest(parts.toArray(String[]::new)));
    }
    public static void validateKey(String key) {
        if (key == null || !key.matches("[A-Za-z0-9._:-]{8,128}")) throw new ServiceFailure(400,"INVALID_IDEMPOTENCY_KEY");
    }
    private static int bytes(String value) { return value.getBytes(StandardCharsets.UTF_8).length; }
    @Override public String toString() { return "SubmissionRequest[destination=" + destinationId + ", payload=REDACTED]"; }
}
