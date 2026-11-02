package com.rcbill.notification.adapters.persistence;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.UUID;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MapperContractTest {
  @Test
  void allStatementsParseAndTenantValuesStayBound() {
    var c = new Configuration();
    c.addMapper(LedgerMapper.class);
    var sql =
        c.getMappedStatement(LedgerMapper.class.getName() + ".dedup")
            .getBoundSql(
                Db.map(
                    "tenant", "' OR 1=1 --", "client", "c", "scope", "submit", "key", "some-key"));
    assertThat(sql.getSql()).doesNotContain("OR 1=1");
    assertThat(sql.getParameterMappings()).hasSize(4);
  }

  @Test
  void invalidTopologyFailsBeforeOpeningAnyConnection() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new Shards(List.of("invalid", "invalid", "invalid"), "", "", false, 16));
  }

  @ParameterizedTest
  @ValueSource(strings = {"task", "lockTask", "payloadRow", "insertChain", "lockChain"})
  void actualUuidParameterBindingMustReachJdbc(String method) throws Exception {
    var configuration = new Configuration();
    configuration.addMapper(LedgerMapper.class);
    var uuid = UUID.randomUUID();
    Object parameters =
        new ParamNameResolver(configuration, LedgerMapper.class.getMethod(method, UUID.class))
            .getNamedParams(new Object[] {uuid});
    var statement = configuration.getMappedStatement(LedgerMapper.class.getName() + "." + method);
    var jdbc = mock(PreparedStatement.class);
    new DefaultParameterHandler(statement, parameters, statement.getBoundSql(parameters))
        .setParameters(jdbc);
    verify(jdbc, atLeastOnce()).setObject(anyInt(), eq(uuid));
  }

  @Test
  void binaryPayloadIsOneRowRatherThanAnArrayOfByteRows() {
    var c = new Configuration();
    c.addMapper(LedgerMapper.class);
    var session = mock(org.apache.ibatis.session.SqlSession.class);
    when(session.getConfiguration()).thenReturn(c);
    byte[] ciphertext = {1, 2, 3, 4};
    when(session.selectOne(anyString(), any()))
        .thenReturn(java.util.Map.of("ciphertext", ciphertext));
    assertThat(c.getMapper(LedgerMapper.class, session).payload(UUID.randomUUID()))
        .isEqualTo(ciphertext);
    verify(session, never()).selectList(anyString(), any());
  }
}
