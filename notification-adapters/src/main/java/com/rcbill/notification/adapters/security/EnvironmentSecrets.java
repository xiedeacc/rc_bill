package com.rcbill.notification.adapters.security;

import com.rcbill.notification.application.port.SecretProvider;
import java.util.Arrays;
import java.util.function.Function;

public final class EnvironmentSecrets implements SecretProvider {
    private final Function<String,String> source;
    public EnvironmentSecrets(Function<String,String> source){this.source=source;}
    @Override public SecretMaterial resolve(String ref){
        if(ref==null||!ref.matches("env:[A-Z][A-Z0-9_]*"))throw new IllegalArgumentException("Untrusted secret reference");
        String value=source.apply(ref.substring(4));
        if(value==null||value.isBlank())throw new IllegalStateException("Provider credential unavailable");
        return new SecretMaterial(){
            private final char[] buffer=value.toCharArray();
            public char[] value(){return buffer;}
            public void close(){Arrays.fill(buffer,'\0');}
            public String toString(){return "SecretMaterial[REDACTED]";}
        };
    }
}
