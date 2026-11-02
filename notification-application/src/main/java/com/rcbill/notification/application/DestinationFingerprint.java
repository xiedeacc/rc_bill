package com.rcbill.notification.application;
import com.rcbill.notification.application.port.DestinationCatalog;
import com.rcbill.notification.domain.Routing;
import java.util.*;
/** Delivery semantics only; enable/disable and caller grants remain operational authorization. */
public final class DestinationFingerprint {
    private DestinationFingerprint(){}
    public static String of(DestinationCatalog.Snapshot d){
        var values=new ArrayList<String>();Collections.addAll(values,d.id(),Integer.toString(d.version()),d.endpoint().toASCIIString(),d.method(),Objects.toString(d.credentialRef(),""),d.credentialHeader().toLowerCase(Locale.ROOT),d.credentialPrefix(),Objects.toString(d.idempotencyHeader(),"").toLowerCase(Locale.ROOT),Integer.toString(d.policy().timeoutSeconds()),Integer.toString(d.policy().maxAttempts()),Long.toString(d.policy().idempotencyWindowSeconds()));
        values.add(Integer.toString(d.contentTypes().size()));values.addAll(new TreeSet<>(d.contentTypes()));
        values.add(Integer.toString(d.allowedDynamicHeaders().size()));values.addAll(new TreeSet<>(d.allowedDynamicHeaders()));
        values.add(Integer.toString(d.staticHeaders().size()));new TreeMap<>(d.staticHeaders()).forEach((k,v)->{values.add(k.toLowerCase(Locale.ROOT));values.add(v);});
        return HexFormat.of().formatHex(Routing.digest(values.toArray(String[]::new)));
    }
}
