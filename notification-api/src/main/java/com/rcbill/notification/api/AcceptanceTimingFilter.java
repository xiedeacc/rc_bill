package com.rcbill.notification.api;

import com.rcbill.notification.adapters.observability.StageTiming;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.IOException;
import org.springframework.web.filter.OncePerRequestFilter;

final class AcceptanceTimingFilter extends OncePerRequestFilter {
  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    if (request.getMethod().equals("POST") && request.getRequestURI().equals("/v1/notifications")) {
      try (var timing = StageTiming.start("http.accept")) {
        chain.doFilter(request, response);
      }
    } else chain.doFilter(request, response);
  }
}
