package com.rcbill.notification.adapters.http;
import java.net.*;
public final class AddressPolicy {
    private final boolean allowLoopback;
    private final URI trustedControlPlane;
    private final java.util.Set<String> additionalOrigins;
    public AddressPolicy(boolean allowLoopback) {this(allowLoopback,null,java.util.Set.of());}
    public AddressPolicy(boolean allowLoopback,java.util.Set<URI> origins) {this(allowLoopback,null,origins);}
    private AddressPolicy(boolean allowLoopback,URI trustedControlPlane,java.util.Set<URI> origins) {
        this.allowLoopback=allowLoopback;this.trustedControlPlane=trustedControlPlane;
        for(var origin:origins)if(origin.getRawQuery()!=null||origin.getRawFragment()!=null||origin.getUserInfo()!=null||!(origin.getPath().isEmpty()||origin.getPath().equals("/")))throw new IllegalArgumentException("Allowlist entries must be origins");
        additionalOrigins=origins.stream().map(AddressPolicy::originKey).collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
    private static String originKey(URI uri){
        if(uri.getHost()==null||uri.getScheme()==null||!java.util.Set.of("http","https").contains(uri.getScheme().toLowerCase(java.util.Locale.ROOT)))throw new IllegalArgumentException("HTTP origin required");
        String scheme=uri.getScheme().toLowerCase(java.util.Locale.ROOT);return scheme+"://"+uri.getHost().toLowerCase(java.util.Locale.ROOT)+":"+(uri.getPort()<0?(scheme.equals("https")?443:80):uri.getPort());
    }
    public static AddressPolicy controlPlane(URI origin,boolean allowLoopback){
        if(origin.getRawQuery()!=null||!(origin.getPath().isEmpty()||origin.getPath().equals("/")))throw new IllegalArgumentException("Control-plane URI must be an origin");
        var policy=new AddressPolicy(allowLoopback,origin,java.util.Set.of());policy.validate(origin);return policy;
    }
    public void validate(URI endpoint) {
        if(endpoint==null||endpoint.getHost()==null||endpoint.getUserInfo()!=null||endpoint.getFragment()!=null)
            throw new IllegalArgumentException("Invalid destination URI");
        if(trustedControlPlane!=null&&(!endpoint.getHost().equalsIgnoreCase(trustedControlPlane.getHost())||endpoint.getPort()!=trustedControlPlane.getPort()||!endpoint.getScheme().equals(trustedControlPlane.getScheme())))throw new IllegalArgumentException("Control-plane authority is fixed");
        if(allowLoopback&&"http".equals(endpoint.getScheme())&&java.util.Set.of("127.0.0.1","[::1]","localhost").contains(endpoint.getHost()))return;
        if(additionalOrigins.contains(originKey(endpoint)))return;
        if(!"https".equals(endpoint.getScheme())||(endpoint.getPort()!=-1&&endpoint.getPort()!=443))throw new IllegalArgumentException("HTTPS 443 required");
    }
    public InetAddress[] validateAddresses(InetAddress[] addresses) throws UnknownHostException {
        if(addresses.length==0)throw new UnknownHostException("Empty DNS response");
        for(InetAddress a:addresses){
            if(allowLoopback&&a.isLoopbackAddress())continue;
            byte[] b=a.getAddress();
            if(trustedControlPlane!=null&&!a.isLinkLocalAddress()&&!a.isLoopbackAddress()&&(a.isSiteLocalAddress()||(b.length==16&&(b[0]&0xfe)==0xfc)))continue;
            boolean invalid=a.isAnyLocalAddress()||a.isLoopbackAddress()||a.isLinkLocalAddress()||a.isSiteLocalAddress()||a.isMulticastAddress();
            if(b.length==4){int x=b[0]&255,y=b[1]&255,z=b[2]&255;
                invalid|=x==0||x>=224||(x==100&&y>=64&&y<=127)||(x==192&&y==0)
                        ||(x==198&&(y==18||y==19||(y==51&&z==100)))||(x==203&&y==0&&z==113);
            }else{invalid|=(b[0]&0xe0)!=0x20||((b[0]&255)==0x20&&(b[1]&255)==1&&(b[2]&255)==0x0d&&(b[3]&255)==0xb8);}
            if(invalid)throw new UnknownHostException("Destination address is not allowed");
        }
        return addresses.clone();
    }
}
