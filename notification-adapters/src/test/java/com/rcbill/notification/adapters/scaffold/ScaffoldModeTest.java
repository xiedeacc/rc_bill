package com.rcbill.notification.adapters.scaffold;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.env.MockEnvironment;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScaffoldModeTest {
    @Test
    void permitsExplicitScaffoldOnLoopback() {
        var env = new MockEnvironment().withProperty("server.address", "127.0.0.1");
        env.setActiveProfiles("scaffold");
        assertThatCode(() -> ScaffoldMode.requireSafe(env)).doesNotThrowAnyException();
    }

    @Test
    void rejectsImplicitDefaultProfile() {
        var env = new MockEnvironment().withProperty("server.address", "127.0.0.1");
        assertThatThrownBy(() -> ScaffoldMode.requireSafe(env)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsProductionMixedWithScaffold() {
        var env = new MockEnvironment().withProperty("server.address", "127.0.0.1");
        env.setActiveProfiles("scaffold", "prod");
        assertThatThrownBy(() -> ScaffoldMode.requireSafe(env)).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"prod", "test", "Scaffold"})
    void rejectsOtherProfiles(String profile) {
        var env = new MockEnvironment().withProperty("server.address", "127.0.0.1");
        env.setActiveProfiles(profile);
        assertThatThrownBy(() -> ScaffoldMode.requireSafe(env)).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.0.0.0", "::", "192.168.1.1", "localhost", ""})
    void rejectsWildcardExternalOrUnresolvedAddresses(String address) {
        var env = new MockEnvironment().withProperty("server.address", address);
        env.setActiveProfiles("scaffold");
        assertThatThrownBy(() -> ScaffoldMode.requireSafe(env)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsMissingBindAddress() {
        var env = new MockEnvironment();
        env.setActiveProfiles("scaffold");
        assertThatThrownBy(() -> ScaffoldMode.requireSafe(env)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void permitsLiteralIpv6Loopback() {
        var env = new MockEnvironment().withProperty("server.address", "::1");
        env.setActiveProfiles("scaffold");
        assertThatCode(() -> ScaffoldMode.requireSafe(env)).doesNotThrowAnyException();
    }

    @Test
    void rejectsExternalManagementListener() {
        var env = new MockEnvironment().withProperty("server.address", "127.0.0.1")
                .withProperty("management.server.address", "0.0.0.0");
        env.setActiveProfiles("scaffold");
        assertThatThrownBy(() -> ScaffoldMode.requireSafe(env)).isInstanceOf(IllegalStateException.class);
    }
}
