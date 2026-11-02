package com.rcbill.notification.adapters.scaffold;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;

/** Local-only, deliberately unavailable runtime. Replace through production-feature TDD. */
@Configuration(proxyBeanMethods = false)
@org.springframework.context.annotation.Profile("scaffold")
public class ScaffoldConfiguration {
  @Bean
  static BeanFactoryPostProcessor scaffoldStartupGuard(Environment environment) {
    // A bean-factory post-processor fails before a web server can accept requests.
    ScaffoldMode.requireSafe(environment);
    return beanFactory -> {};
  }

  @Bean
  ScaffoldHealthIndicator scaffoldHealthIndicator() {
    return new ScaffoldHealthIndicator();
  }

  @Bean
  SecurityFilterChain scaffoldSecurity(HttpSecurity http) throws Exception {
    // Only local health checks and explicit 501 routes are exposed. No business auth exists.
    return http.csrf(AbstractHttpConfigurer::disable)
        .httpBasic(AbstractHttpConfigurer::disable)
        .formLogin(AbstractHttpConfigurer::disable)
        .logout(AbstractHttpConfigurer::disable)
        .sessionManagement(
            session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(
            auth ->
                auth.requestMatchers(
                        "/actuator/health",
                        "/actuator/health/**",
                        "/v1/notifications",
                        "/v1/notifications/**")
                    .permitAll()
                    .anyRequest()
                    .denyAll())
        .build();
  }
}
