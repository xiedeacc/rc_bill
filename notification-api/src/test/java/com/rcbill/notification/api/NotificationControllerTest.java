package com.rcbill.notification.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpMethod;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class NotificationControllerTest {
  private static final String ID = "018f7fa4-3b61-7498-bf9e-c2b34bfdbf5d";

  static Stream<Arguments> businessRoutes() {
    return Stream.of(
        Arguments.of("POST", "/v1/notifications"),
        Arguments.of("GET", "/v1/notifications/" + ID),
        Arguments.of("GET", "/v1/notifications/" + ID + "/attempts"),
        Arguments.of("POST", "/v1/notifications/" + ID + "/replays"));
  }

  @ParameterizedTest
  @MethodSource("businessRoutes")
  void unfinishedRoutesNeverClaimAcceptance(String method, String path) throws Exception {
    var mvc = MockMvcBuilders.standaloneSetup(new NotificationController()).build();
    mvc.perform(
            request(HttpMethod.valueOf(method), path).contentType("application/json").content("{}"))
        .andExpect(status().isNotImplemented())
        .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
        .andExpect(header().doesNotExist("Location"))
        .andExpect(jsonPath("$.status").value(501))
        .andExpect(jsonPath("$.code").value("NOT_IMPLEMENTED"))
        .andExpect(jsonPath("$.instance").value(path))
        .andExpect(jsonPath("$.traceId").isNotEmpty());
  }
}
