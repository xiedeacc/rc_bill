package com.rcbill.notification.adapters.http;
import java.net.*;
public final class AddressPolicy {
    private final boolean allowLoopback;
    public AddressPolicy(boolean allowLoopback) {this.allowLoopback=allowLoopback;}
    public void validate(URI endpoint) {
        if(endpoint==null||endpoint.getHost()==null||endpoint.getUserInfo()!=null||endpoint.getFragment()!=null)
            throw new IllegalArgumentException("Invalid destination URI");
        if(allowLoopback&&"http".equals(endpoint.getScheme())&&java.util.Set.of("127.0.0.1","[::1]","localhost").contains(endpoint.getHost()))return;
        if(!"https".equals(endpoint.getScheme())||(endpoint.getPort()!=-1&&endpoint.getPort()!=443))throw new IllegalArgumentException("HTTPS 443 required");
    }
    public InetAddress[] validateAddresses(InetAddress[] addresses) throws UnknownHostException {
        if(addresses.length==0)throw new UnknownHostException("Empty DNS response");
        for(InetAddress a:addresses){
            if(allowLoopback&&a.isLoopbackAddress())continue;
            byte[] b=a.getAddress();
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
