package com.rcbill.notification.adapters.scaffold;

import org.springframework.core.env.Environment;

public final class ScaffoldMode {
  private ScaffoldMode() {}

  public static void requireSafe(Environment environment) {
    String[] profiles = environment.getActiveProfiles();
    if (profiles.length != 1 || !"scaffold".equals(profiles[0])) {
      throw new IllegalStateException("Only the explicit scaffold profile is implemented.");
    }
    String address = environment.getProperty("server.address");
    if (!"127.0.0.1".equals(address) && !"::1".equals(address)) {
      throw new IllegalStateException("Scaffold must bind to a literal loopback address.");
    }
    String managementAddress = environment.getProperty("management.server.address");
    if (managementAddress != null
        && !"127.0.0.1".equals(managementAddress)
        && !"::1".equals(managementAddress)) {
      throw new IllegalStateException("Scaffold management must remain on loopback.");
    }
  }
}
