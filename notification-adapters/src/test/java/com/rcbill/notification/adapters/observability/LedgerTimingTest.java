package com.rcbill.notification.adapters.observability;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LedgerTimingTest {
  @TempDir Path temporary;

  @Test
  void recordsFixedStatementNameOnSuccessAndFailureWithoutArguments() throws Exception {
    var delegate = mock(Executor.class);
    var mapped =
        new MappedStatement.Builder(
                new Configuration(), "ledger.insertTask", parameter -> null, SqlCommandType.INSERT)
            .build();
    var failure = new IllegalStateException("private detail");
    when(delegate.update(mapped, "private input")).thenReturn(1).thenThrow(failure);
    var proxy = (Executor) new LedgerTiming().plugin(delegate);
    try (var recording = new Recording()) {
      recording.enable("rcbill.Stage");
      recording.start();
      assertThat(proxy.update(mapped, "private input")).isEqualTo(1);
      assertThatThrownBy(() -> proxy.update(mapped, "private input")).isSameAs(failure);
      recording.stop();
      var file = temporary.resolve("events.jfr");
      recording.dump(file);
      var events =
          RecordingFile.readAllEvents(file).stream()
              .filter(e -> e.getEventType().getName().equals("rcbill.Stage"))
              .toList();
      assertThat(events).hasSize(2);
      assertThat(events)
          .allSatisfy(
              e -> {
                assertThat(e.getString("stage")).isEqualTo("sql.insertTask");
                assertThat(e.toString()).doesNotContain("private input", "private detail");
                assertThat(e.getDuration().toNanos()).isNotNegative();
              });
    }
  }
}
