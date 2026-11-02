package com.rcbill.notification.api;

import jakarta.servlet.http.*;
import java.io.IOException;
import java.util.*;
import tools.jackson.databind.json.JsonMapper;

final class ApiProblems {
  private static final JsonMapper JSON = JsonMapper.builder().build();

  static void write(
      HttpServletRequest request, HttpServletResponse response, int status, String code)
      throws IOException {
    response.setStatus(status);
    response.setContentType("application/problem+json");
    response.setCharacterEncoding("UTF-8");
    if (status == 401) response.setHeader("WWW-Authenticate", "Bearer");
    response
        .getWriter()
        .write(
            JSON.writeValueAsString(
                Map.of(
                    "type",
                    "urn:rcbill:problem:" + code.toLowerCase(Locale.ROOT),
                    "title",
                    code,
                    "status",
                    status,
                    "detail",
                    code,
                    "instance",
                    request.getRequestURI(),
                    "code",
                    code,
                    "traceId",
                    UUID.randomUUID().toString())));
  }
}
