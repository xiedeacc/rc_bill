package com.rcbill.notification.adapters.persistence;

import java.time.*;
import java.util.*;

public final class Db {
    private Db() { }
    public static Map<String,Object> map(Object... pairs) {
        var out=new HashMap<String,Object>();
        for(int i=0;i<pairs.length;i+=2) out.put((String)pairs[i],pairs[i+1]);
        return out;
    }
    public static Instant instant(Object value) {
        if(value==null)return null;
        if(value instanceof Instant i)return i;
        if(value instanceof OffsetDateTime t)return t.toInstant();
        if(value instanceof java.sql.Timestamp t)return t.toInstant();
        throw new IllegalArgumentException("Unsupported timestamp type");
    }
    public static UUID id(Object value) { return value==null?null:UUID.fromString(value.toString()); }
    public static int integer(Map<String,Object> row,String key) {return ((Number)row.get(key)).intValue();}
    public static long number(Map<String,Object> row,String key) {return ((Number)row.get(key)).longValue();}
}
