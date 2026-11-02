package com.rcbill.notification.api;

import com.rcbill.notification.application.*;
import java.util.*;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

public final class ApiJson {
  private final JsonMapper json =
      JsonMapper.builder(
              JsonFactory.builder()
                  .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                  .streamReadConstraints(
                      StreamReadConstraints.builder()
                          .maxNestingDepth(20)
                          .maxStringLength(131072)
                          .build())
                  .build())
          .build();

  private JsonNode parse(byte[] bytes, Set<String> fields) {
    if (bytes.length > 131072) throw new ServiceFailure(413, "ENVELOPE_TOO_LARGE");
    try {
      var node = json.readTree(bytes);
      if (node == null
          || !node.isObject()
          || node.properties().stream().anyMatch(e -> !fields.contains(e.getKey())))
        throw new IllegalArgumentException();
      return node;
    } catch (RuntimeException e) {
      throw new ServiceFailure(400, "INVALID_JSON");
    }
  }

  private static String text(JsonNode node, String field) {
    var value = node.get(field);
    if (value == null || !value.isString()) throw new ServiceFailure(400, "INVALID_FIELD");
    return value.stringValue();
  }

  private static int integer(JsonNode node, String field) {
    var value = node.get(field);
    if (value == null || !value.isIntegralNumber() || !value.canConvertToInt())
      throw new ServiceFailure(400, "INVALID_FIELD");
    return value.intValue();
  }

  public SubmissionRequest submission(byte[] bytes) {
    var n =
        parse(
            bytes,
            Set.of(
                "destinationId",
                "destinationVersion",
                "contentType",
                "body",
                "headers",
                "ttlSeconds"));
    Map<String, String> headers = new LinkedHashMap<>();
    if (n.has("headers")) {
      var h = n.get("headers");
      if (!h.isObject()) throw new ServiceFailure(400, "INVALID_HEADER");
      for (var e : h.properties()) {
        if (!e.getValue().isString()) throw new ServiceFailure(400, "INVALID_HEADER");
        headers.put(e.getKey(), e.getValue().stringValue());
      }
    }
    return new SubmissionRequest(
            text(n, "destinationId"),
            integer(n, "destinationVersion"),
            text(n, "contentType"),
            text(n, "body"),
            headers,
            n.has("ttlSeconds") ? integer(n, "ttlSeconds") : null)
        .validated();
  }

  public NotificationUseCases.Replay replay(byte[] bytes) {
    var n = parse(bytes, Set.of("reason", "acknowledgeDuplicateRisk"));
    boolean risk = false;
    if (n.has("acknowledgeDuplicateRisk")) {
      if (!n.get("acknowledgeDuplicateRisk").isBoolean())
        throw new ServiceFailure(400, "INVALID_FIELD");
      risk = n.get("acknowledgeDuplicateRisk").booleanValue();
    }
    return new NotificationUseCases.Replay(text(n, "reason"), risk);
  }
}
