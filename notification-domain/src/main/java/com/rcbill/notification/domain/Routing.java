package com.rcbill.notification.domain;

import java.util.UUID;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

public final class Routing {
    private Routing() { }
    public static int slot(String tenant, String client, String scope, String key) {
        byte[] digest = digest(tenant, client, scope, key);
        return ((digest[0] & 255) << 4) | ((digest[1] & 255) >>> 4);
    }
    /** SHA-256 over big-endian byte lengths followed by exact UTF-8 bytes. */
    public static byte[] digest(String... values) {
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            for (String value : values) {
                byte[] encoded = java.util.Objects.requireNonNull(value).getBytes(StandardCharsets.UTF_8);
                out.writeInt(encoded.length);
                out.write(encoded);
            }
            return MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
        } catch (java.io.IOException | java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
    public static UUID newId(int slot) {
        return newId(slot,java.time.Instant.now());
    }
    public static UUID newId(int slot,java.time.Instant time) {
        if (slot < 0 || slot > 4095) throw new IllegalArgumentException("Invalid logical slot");
        long millis=time.toEpochMilli();if(millis<0||millis>0xffffffffffffL)throw new IllegalArgumentException("Timestamp outside UUID layout");
        UUID random = UUID.randomUUID();
        long high = ((long)slot<<52) | ((millis>>>12)<<16) | 0x8000L | (millis&0xfffL);
        return new UUID(high, random.getLeastSignificantBits());
    }
    public static int slot(UUID id) { return (int) (id.getMostSignificantBits() >>> 52); }
}
