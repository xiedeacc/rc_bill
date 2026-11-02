package com.rcbill.notification.adapters.config;

import com.rcbill.notification.application.port.DestinationCatalog;
import com.rcbill.notification.application.port.DestinationCatalog.DurationPolicy;
import java.net.URI;
import java.util.*;

/** Versioned non-secret snapshot. Credential material is never part of this record. */
public record Destination(
    String id,
    int version,
    Set<String> allowedTenants,
    Set<String> allowedClients,
    boolean enabled,
    URI endpoint,
    String method,
    Set<String> contentTypes,
    Set<String> allowedDynamicHeaders,
    Map<String, String> staticHeaders,
    String credentialRef,
    String idempotencyHeader,
    DurationPolicy policy,
    String credentialHeader,
    String credentialPrefix)
    implements DestinationCatalog.Snapshot {
  public Destination(
      String id,
      int version,
      Set<String> tenants,
      Set<String> clients,
      boolean enabled,
      URI endpoint,
      String method,
      Set<String> contentTypes,
      Set<String> dynamic,
      Map<String, String> headers,
      String ref,
      String idempotency,
      DurationPolicy policy) {
    this(
        id,
        version,
        tenants,
        clients,
        enabled,
        endpoint,
        method,
        contentTypes,
        dynamic,
        headers,
        ref,
        idempotency,
        policy,
        null,
        null);
  }

  public Destination {
    credentialHeader = credentialHeader == null ? "Authorization" : credentialHeader;
    credentialPrefix = credentialPrefix == null ? "Bearer " : credentialPrefix;
    if (!credentialHeader.matches("[A-Za-z][A-Za-z0-9-]{0,63}")
        || Set.of("host", "content-length", "transfer-encoding", "connection", "content-type")
            .contains(credentialHeader.toLowerCase(Locale.ROOT))
        || credentialPrefix.contains("\r")
        || credentialPrefix.contains("\n"))
      throw new IllegalArgumentException("Invalid credential binding");
    if (id == null
        || !id.matches("[a-z][a-z0-9-]{2,63}")
        || version < 1
        || endpoint == null
        || endpoint.getHost() == null
        || endpoint.getUserInfo() != null
        || endpoint.getFragment() != null
        || !Set.of("POST", "PUT", "PATCH").contains(method))
      throw new IllegalArgumentException("Invalid destination configuration");
    allowedTenants = Set.copyOf(allowedTenants);
    allowedClients = Set.copyOf(allowedClients);
    contentTypes = Set.copyOf(contentTypes);
    allowedDynamicHeaders =
        allowedDynamicHeaders.stream()
            .map(v -> v.toLowerCase(Locale.ROOT))
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    var normalizedStatic = new TreeMap<String, String>();
    staticHeaders.forEach(
        (k, v) -> {
          if (normalizedStatic.putIfAbsent(k.toLowerCase(Locale.ROOT), v) != null)
            throw new IllegalArgumentException("Duplicate static header");
        });
    staticHeaders = Map.copyOf(normalizedStatic);
    if (policy == null
        || policy.timeoutSeconds() < 1
        || policy.timeoutSeconds() > 10
        || policy.maxAttempts() < 1
        || policy.maxAttempts() > 20)
      throw new IllegalArgumentException("Invalid delivery budgets");
    if (credentialRef != null && !credentialRef.matches("env:[A-Z][A-Z0-9_]*"))
      throw new IllegalArgumentException("Unsupported credential reference");
    if (idempotencyHeader != null && !idempotencyHeader.matches("[A-Za-z][A-Za-z0-9-]{0,63}"))
      throw new IllegalArgumentException("Invalid provider idempotency header");
    if (idempotencyHeader != null) {
      String name = idempotencyHeader.toLowerCase(Locale.ROOT);
      if (idempotencyHeader.equalsIgnoreCase(credentialHeader)
          || Set.of(
                  "authorization",
                  "host",
                  "cookie",
                  "content-type",
                  "content-length",
                  "transfer-encoding",
                  "connection")
              .contains(name)
          || name.startsWith("proxy-")
          || name.startsWith("x-forwarded-")) {
        throw new IllegalArgumentException("Idempotency header conflicts with a protected header");
      }
    }
    staticHeaders.forEach(
        (k, v) -> {
          if (!k.matches("[A-Za-z][A-Za-z0-9-]{0,63}")
              || v.contains("\r")
              || v.contains("\n")
              || Set.of(
                      "authorization",
                      "host",
                      "cookie",
                      "content-length",
                      "transfer-encoding",
                      "connection")
                  .contains(k.toLowerCase(Locale.ROOT)))
            throw new IllegalArgumentException("Invalid static header");
        });
  }
}
