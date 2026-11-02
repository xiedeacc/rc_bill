package com.rcbill.notification.adapters.config;

import com.rcbill.notification.application.*;
import com.rcbill.notification.application.port.DestinationCatalog;
import java.nio.file.*;
import java.util.*;
import tools.jackson.databind.json.JsonMapper;

public final class ConfiguredCatalog implements DestinationCatalog {
  private final Map<String, Destination> destinations;

  public ConfiguredCatalog(List<Destination> entries) {
    var map = new HashMap<String, Destination>();
    for (var d : entries)
      if (map.putIfAbsent(d.id() + ":" + d.version(), d) != null)
        throw new IllegalArgumentException("Duplicate destination version");
    destinations = Map.copyOf(map);
  }

  public static ConfiguredCatalog load(Path path, JsonMapper json) throws java.io.IOException {
    return new ConfiguredCatalog(
        Arrays.asList(json.readValue(Files.readString(path), Destination[].class)));
  }

  @Override
  public Destination resolveAuthorized(NotificationUseCases.Caller caller, String id, int version) {
    var d = destinations.get(id + ":" + version);
    if (d == null
        || !d.enabled()
        || !d.allowedTenants().contains(caller.tenantId())
        || !d.allowedClients().contains(caller.clientId()))
      throw new ServiceFailure(422, "INVALID_DESTINATION");
    return d;
  }

  public boolean enabled(String id, int version) {
    var d = destinations.get(id + ":" + version);
    return d != null && d.enabled();
  }

  public Optional<Destination> enabledDestination(String id, int version) {
    return Optional.ofNullable(destinations.get(id + ":" + version)).filter(Destination::enabled);
  }

  public Collection<Destination> all() {
    return destinations.values();
  }
}
