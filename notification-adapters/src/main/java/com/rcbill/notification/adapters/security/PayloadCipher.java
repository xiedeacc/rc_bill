package com.rcbill.notification.adapters.security;
import java.util.UUID;
import java.util.Base64;
import java.util.Arrays;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
public final class PayloadCipher {
    private final SecretKeySpec key;
    private final SecureRandom random=new SecureRandom();
    public PayloadCipher(String base64Key) {
        byte[] raw=Base64.getDecoder().decode(base64Key);
        if(raw.length!=32)throw new IllegalArgumentException("Encryption key must be 256 bits");
        key=new SecretKeySpec(raw,"AES");Arrays.fill(raw,(byte)0);
    }
    public byte[] encrypt(UUID id,String value) {
        try {
            byte[] nonce=new byte[12];random.nextBytes(nonce);
            Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.ENCRYPT_MODE,key,new GCMParameterSpec(128,nonce));
            cipher.updateAAD(id.toString().getBytes(StandardCharsets.US_ASCII));
            byte[] encrypted=cipher.doFinal(value.getBytes(StandardCharsets.UTF_8));
            byte[] result=Arrays.copyOf(nonce,nonce.length+encrypted.length);System.arraycopy(encrypted,0,result,nonce.length,encrypted.length);return result;
        } catch(Exception e){throw new IllegalStateException("Payload encryption failed",e);}
    }
    public String decrypt(UUID id,byte[] value) {
        try {
            if(value.length<28)throw new IllegalArgumentException("Truncated payload");
            Cipher cipher=Cipher.getInstance("AES/GCM/NoPadding");cipher.init(Cipher.DECRYPT_MODE,key,new GCMParameterSpec(128,Arrays.copyOf(value,12)));
            cipher.updateAAD(id.toString().getBytes(StandardCharsets.US_ASCII));
            return new String(cipher.doFinal(value,12,value.length-12),StandardCharsets.UTF_8);
        } catch(Exception e){throw new IllegalStateException("Payload authentication failed",e);}
    }
}
