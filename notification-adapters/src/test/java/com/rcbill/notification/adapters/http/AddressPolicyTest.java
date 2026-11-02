package com.rcbill.notification.adapters.http;
import java.net.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
class AddressPolicyTest {
    @ParameterizedTest @ValueSource(strings={"127.0.0.1","0.0.0.0","10.1.2.3","172.16.0.1","192.168.1.1","169.254.169.254","100.64.0.1","198.18.0.1","224.0.0.1","::1","fc00::1","fe80::1","::ffff:127.0.0.1","2001:db8::1"})
    void blocksNonPublicAddresses(String ip) throws Exception {
        var addresses=new InetAddress[]{InetAddress.getByName(ip)};
        assertThatThrownBy(()->new AddressPolicy(false).validateAddresses(addresses)).isInstanceOf(UnknownHostException.class);
    }
    @Test void mixedDnsAnswersCannotBypassValidation() throws Exception {
        var addresses=new InetAddress[]{InetAddress.getByName("8.8.8.8"),InetAddress.getByName("10.0.0.1")};
        assertThatThrownBy(()->new AddressPolicy(false).validateAddresses(addresses)).isInstanceOf(UnknownHostException.class);
    }
    @Test void loopbackExceptionIsExplicitAndLimited() throws Exception {
        new AddressPolicy(true).validateAddresses(new InetAddress[]{InetAddress.getByName("127.0.0.1")});
        assertThatThrownBy(()->new AddressPolicy(true).validateAddresses(new InetAddress[]{InetAddress.getByName("10.0.0.1")})).isInstanceOf(UnknownHostException.class);
    }
    @ParameterizedTest @ValueSource(strings={"http://example.com/x","https://user:pass@example.com/x","https://example.com:8443/x","https://example.com/x#fragment","file:///etc/passwd"})
    void rejectsUnsafeUrlForms(String url) {assertThatIllegalArgumentException().isThrownBy(()->new AddressPolicy(false).validate(URI.create(url)));}
    @Test void permitsNormalHttps(){new AddressPolicy(false).validate(URI.create("https://example.com/v1"));}
}
