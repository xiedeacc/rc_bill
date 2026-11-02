package com.rcbill.notification.application.port;

/** Only references obtained from trusted destination configuration are accepted. */
public interface SecretProvider {
  SecretMaterial resolve(String credentialRef);

  /** Implementations wipe mutable buffers on close; never expose secrets via toString. */
  interface SecretMaterial extends AutoCloseable {
    char[] value();

    @Override
    void close();
  }
}
