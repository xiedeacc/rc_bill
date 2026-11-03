package com.rcbill.notification.adapters.observability;

import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.session.*;

@Intercepts({
  @Signature(
      type = Executor.class,
      method = "update",
      args = {MappedStatement.class, Object.class}),
  @Signature(
      type = Executor.class,
      method = "query",
      args = {MappedStatement.class, Object.class, RowBounds.class, ResultHandler.class})
})
public final class LedgerTiming implements Interceptor {
  public Object intercept(Invocation invocation) throws Throwable {
    String id = ((MappedStatement) invocation.getArgs()[0]).getId();
    try (var timing = StageTiming.start("sql." + id.substring(id.lastIndexOf('.') + 1))) {
      return invocation.proceed();
    }
  }
}
