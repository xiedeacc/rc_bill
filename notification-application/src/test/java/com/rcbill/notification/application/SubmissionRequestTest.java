package com.rcbill.notification.application;

import java.util.Map;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class SubmissionRequestTest {
    SubmissionRequest request(String body, Map<String,String> headers, Integer ttl) {
        return new SubmissionRequest("crm-contact", 1, "application/json", body, headers, ttl);
    }
    @Test void normalizesHeaderNamesAndDefaultsButPreservesBodyBytes() {
        var a = request("{}", Map.of("X-ID", "1"), null).validated();
        var b = request("{}", Map.of("x-id", "1"), 86400).validated();
        assertThat(a.fingerprint()).isEqualTo(b.fingerprint());
        assertThat(a.fingerprint()).isNotEqualTo(request("{ }", Map.of("x-id", "1"), 86400).validated().fingerprint());
        assertThat(a.headers()).containsOnlyKeys("x-id");
    }
    @Test void boundsAreBytesNotCharacters() {
        request("a".repeat(65536), Map.of(), 60).validated();
        assertThatThrownBy(() -> request("中".repeat(22000), Map.of(), 60).validated())
                .isInstanceOf(ServiceFailure.class);
    }
    @ParameterizedTest @ValueSource(strings={"Host","Authorization","Cookie","Content-Length","Transfer-Encoding","Proxy-X","X-Forwarded-For","Connection","Content-Type"})
    void refusesControlledHeaders(String name) {
        assertThatThrownBy(() -> request("", Map.of(name,"x"),60).validated()).isInstanceOf(ServiceFailure.class);
    }
    @Test void refusesCaseDuplicatesAndNewlines() {
        assertThatThrownBy(() -> request("",Map.of("X-A","1","x-a","2"),60).validated()).isInstanceOf(ServiceFailure.class);
        assertThatThrownBy(() -> request("",Map.of("X-A","\r\nHost: bad"),60).validated()).isInstanceOf(ServiceFailure.class);
    }
    @ParameterizedTest @ValueSource(ints={0,59,86401})
    void rejectsBadTtl(int ttl) {
        assertThatThrownBy(() -> request("",Map.of(),ttl).validated()).isInstanceOf(ServiceFailure.class);
    }
    @Test void snapshotsMutableHeadersAndRedactsDiagnosticText() {
        var headers = new LinkedHashMap<String,String>(); headers.put("X-A","private-header");
        var value = request("private-body",headers,60).validated(); headers.clear();
        assertThat(value.headers()).hasSize(1);
        assertThat(value.toString()).doesNotContain("private-body","private-header");
        assertThatThrownBy(() -> value.headers().put("x-b","x")).isInstanceOf(UnsupportedOperationException.class);
    }
    @Test void rejectsUnpairedSurrogatesAndNonPortableHeaderValues(){
        assertThatThrownBy(()->request("\ud800",Map.of(),60).validated()).isInstanceOf(ServiceFailure.class);
        assertThatThrownBy(()->request("",Map.of("X-Event","中文"),60).validated()).isInstanceOf(ServiceFailure.class);
    }
}
