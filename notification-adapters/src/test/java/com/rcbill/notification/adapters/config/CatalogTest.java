package com.rcbill.notification.adapters.config;

import static org.assertj.core.api.Assertions.*;

import com.rcbill.notification.adapters.security.EnvironmentSecrets;
import com.rcbill.notification.application.*;
import com.rcbill.notification.application.port.DestinationCatalog;
import java.net.URI;
import java.util.*;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class CatalogTest {
  Destination target() {
    return new Destination(
        "crm-test",
        1,
        Set.of("t"),
        Set.of("c"),
        true,
        URI.create("https://example.com/notify"),
        "POST",
        Set.of("text/plain"),
        Set.of("X-Event"),
        Map.of(),
        "env:PROVIDER_KEY",
        "Idempotency-Key",
        new DestinationCatalog.DurationPolicy(10, 20, 86400),
        "X-API-Key",
        "");
  }

  @Test
  void ownershipAndVersionAreRequiredAndSnapshotsRoundTrip() {
    var d = target();
    var catalog = new ConfiguredCatalog(List.of(d));
    var owner = new NotificationUseCases.Caller("t", "c", "s");
    assertThat(catalog.resolveAuthorized(owner, "crm-test", 1)).isEqualTo(d);
    assertThatThrownBy(
            () ->
                catalog.resolveAuthorized(
                    new NotificationUseCases.Caller("other", "c", "s"), "crm-test", 1))
        .isInstanceOf(ServiceFailure.class);
    assertThatThrownBy(() -> catalog.resolveAuthorized(owner, "crm-test", 2))
        .isInstanceOf(ServiceFailure.class);
    assertThatIllegalArgumentException().isThrownBy(() -> new ConfiguredCatalog(List.of(d, d)));
    var json = JsonMapper.builder().build();
    assertThat(json.readValue(json.writeValueAsString(d), Destination.class)).isEqualTo(d);
    assertThat(d.credentialPrefix()).isEmpty();
    assertThat(d.allowedDynamicHeaders()).containsExactly("x-event");
  }

  @Test
  void secretsAreRestrictedRedactedAndWiped() {
    var provider = new EnvironmentSecrets(k -> k.equals("PROVIDER_KEY") ? "sensitive" : null);
    var secret = provider.resolve("env:PROVIDER_KEY");
    char[] bytes = secret.value();
    assertThat(secret.toString()).doesNotContain("sensitive");
    secret.close();
    assertThat(bytes).containsOnly('\0');
    assertThatIllegalArgumentException()
        .isThrownBy(() -> provider.resolve("http://attacker/secret"));
    assertThatIllegalStateException().isThrownBy(() -> provider.resolve("env:MISSING"));
  }

  @Test
  void deliveryIdentityIgnoresPauseButIncludesActualTarget() {
    var d = target();
    var paused =
        new Destination(
            d.id(),
            d.version(),
            d.allowedTenants(),
            d.allowedClients(),
            false,
            d.endpoint(),
            d.method(),
            d.contentTypes(),
            d.allowedDynamicHeaders(),
            d.staticHeaders(),
            d.credentialRef(),
            d.idempotencyHeader(),
            d.policy(),
            d.credentialHeader(),
            d.credentialPrefix());
    assertThat(DestinationFingerprint.of(d)).isEqualTo(DestinationFingerprint.of(paused));
    var changed =
        new Destination(
            d.id(),
            d.version(),
            d.allowedTenants(),
            d.allowedClients(),
            true,
            URI.create("https://other.example.com/notify"),
            d.method(),
            d.contentTypes(),
            d.allowedDynamicHeaders(),
            d.staticHeaders(),
            d.credentialRef(),
            d.idempotencyHeader(),
            d.policy(),
            d.credentialHeader(),
            d.credentialPrefix());
    assertThat(DestinationFingerprint.of(d)).isNotEqualTo(DestinationFingerprint.of(changed));
  }
}
