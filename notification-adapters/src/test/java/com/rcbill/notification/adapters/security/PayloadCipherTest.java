package com.rcbill.notification.adapters.security;

import static org.assertj.core.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;

class PayloadCipherTest {
  final String key = Base64.getEncoder().encodeToString(new byte[32]);

  @Test
  void ciphertextIsRandomizedAndBoundToTask() {
    var cipher = new PayloadCipher(key);
    var id = UUID.randomUUID();
    byte[] a = cipher.encrypt(id, "private-中文"), b = cipher.encrypt(id, "private-中文");
    assertThat(a).isNotEqualTo(b);
    assertThat(cipher.decrypt(id, a)).isEqualTo("private-中文");
    assertThatThrownBy(() -> cipher.decrypt(UUID.randomUUID(), a))
        .isInstanceOf(IllegalStateException.class);
    a[a.length - 1] ^= 1;
    assertThatThrownBy(() -> cipher.decrypt(id, a)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void invalidKeysAndTruncatedCiphertextFailClosed() {
    assertThatIllegalArgumentException().isThrownBy(() -> new PayloadCipher(""));
    assertThatIllegalStateException()
        .isThrownBy(() -> new PayloadCipher(key).decrypt(UUID.randomUUID(), new byte[3]));
  }
}
