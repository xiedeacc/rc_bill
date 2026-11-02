package com.rcbill.notification.adapters.config;

import com.rcbill.notification.adapters.scaffold.ScaffoldMode;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;

@Configuration(proxyBeanMethods = false)
public class StartupConfiguration {
  @Bean
  static BeanFactoryPostProcessor modeGuard(Environment env) {
    String[] modes = env.getActiveProfiles();
    if (modes.length != 1 || !java.util.Set.of("scaffold", "runtime").contains(modes[0]))
      throw new IllegalStateException("Choose exactly one supported profile: scaffold or runtime");
    if (modes[0].equals("scaffold")) ScaffoldMode.requireSafe(env);
    else if (env.getProperty("notification.allow-loopback", Boolean.class, false)
        && !java.util.Set.of("127.0.0.1", "::1").contains(env.getProperty("server.address", "")))
      throw new IllegalStateException("Loopback test exception requires a loopback listener");
    return factory -> {};
  }
}
