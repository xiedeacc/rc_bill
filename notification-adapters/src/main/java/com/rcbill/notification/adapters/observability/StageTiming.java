package com.rcbill.notification.adapters.observability;

import jdk.jfr.*;

/** Fixed operation names only: never attach identifiers, SQL arguments, or request data. */
@Name("rcbill.Stage")
@Label("Notification stage")
@Category("Notification")
@StackTrace(false)
public final class StageTiming extends Event implements AutoCloseable {
  public String stage;

  private StageTiming(String stage) {
    this.stage = stage;
    begin();
  }

  public static StageTiming start(String stage) {
    return new StageTiming(stage);
  }

  public void close() {
    end();
    commit();
  }
}
