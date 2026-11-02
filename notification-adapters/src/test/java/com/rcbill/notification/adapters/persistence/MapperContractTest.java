package com.rcbill.notification.adapters.persistence;

import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class MapperContractTest {
    @Test void allStatementsParseAndTenantValuesStayBound() {
        var c=new Configuration();c.addMapper(LedgerMapper.class);
        var sql=c.getMappedStatement(LedgerMapper.class.getName()+".dedup")
                .getBoundSql(Db.map("tenant","' OR 1=1 --","client","c","scope","submit","key","some-key"));
        assertThat(sql.getSql()).doesNotContain("OR 1=1");
        assertThat(sql.getParameterMappings()).hasSize(4);
    }
    @Test void invalidTopologyFailsBeforeOpeningAnyConnection() {
        assertThatIllegalArgumentException().isThrownBy(()->new Shards(List.of("invalid","invalid","invalid"),"","",false,16));
    }
}
