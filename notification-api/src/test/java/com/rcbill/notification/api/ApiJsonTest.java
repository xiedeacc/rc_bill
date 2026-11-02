package com.rcbill.notification.api;
import com.rcbill.notification.application.ServiceFailure;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
class ApiJsonTest {
    final ApiJson parser=new ApiJson();
    byte[] bytes(String s){return s.getBytes(StandardCharsets.UTF_8);}
    @Test void preservesRawBodyAndAppliesDefaults(){var s=parser.submission(bytes("{\"destinationId\":\"crm-test\",\"destinationVersion\":1,\"contentType\":\"text/plain\",\"body\":\"  中  \"}"));assertThat(s.body()).isEqualTo("  中  ");assertThat(s.ttlSeconds()).isEqualTo(86400);}
    @ParameterizedTest @ValueSource(strings={"{}","[]","{\"a\":1,\"a\":2}","{\"destinationId\":\"crm-test\",\"destinationVersion\":\"1\",\"contentType\":\"text/plain\",\"body\":\"x\"}","{\"destinationId\":\"crm-test\",\"destinationVersion\":1,\"contentType\":\"text/plain\",\"body\":\"x\",\"tenantId\":\"attacker\"}"})
    void malformedUnknownAndCoercedInputsFail(String value){assertThatThrownBy(()->parser.submission(bytes(value))).isInstanceOf(ServiceFailure.class);}
    @Test void replayDoesNotCoerceRiskAcknowledgement(){assertThatThrownBy(()->parser.replay(bytes("{\"reason\":\"repair\",\"acknowledgeDuplicateRisk\":\"true\"}"))).isInstanceOf(ServiceFailure.class);assertThat(parser.replay(bytes("{\"reason\":\"repair\",\"acknowledgeDuplicateRisk\":true}")).acknowledgeDuplicateRisk()).isTrue();}
}
