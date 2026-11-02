package com.rcbill.notification.application;

public final class ServiceFailure extends RuntimeException {
    private final int status;
    private final String code;
    public ServiceFailure(int status, String code) { super(code); this.status = status; this.code = code; }
    public int status() { return status; }
    public String code() { return code; }
}
