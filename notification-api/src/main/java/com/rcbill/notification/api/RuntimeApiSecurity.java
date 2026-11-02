package com.rcbill.notification.api;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.web.SecurityFilterChain;
import java.nio.file.*;
import java.security.*;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
@Configuration(proxyBeanMethods=false) @Profile("runtime")
public class RuntimeApiSecurity {
    @Bean JwtDecoder jwtDecoder(Environment env)throws Exception{
        String issuer=env.getRequiredProperty("notification.security.issuer"),audience=env.getRequiredProperty("notification.security.audience");
        String pem=env.getProperty("notification.security.public-key-file");NimbusJwtDecoder decoder;
        if(pem!=null){String key=Files.readString(Path.of(pem)).replace("-----BEGIN PUBLIC KEY-----","").replace("-----END PUBLIC KEY-----","").replaceAll("\\s","");decoder=NimbusJwtDecoder.withPublicKey((RSAPublicKey)KeyFactory.getInstance("RSA").generatePublic(new X509EncodedKeySpec(Base64.getDecoder().decode(key)))).signatureAlgorithm(SignatureAlgorithm.RS256).build();}
        else decoder=NimbusJwtDecoder.withJwkSetUri(env.getRequiredProperty("notification.security.jwk-set-uri")).jwsAlgorithm(SignatureAlgorithm.RS256).build();
        OAuth2TokenValidator<Jwt> claims=token->{boolean valid=token.getAudience().contains(audience)&&token.getExpiresAt()!=null&&token.getSubject()!=null&&token.getClaimAsString("tenant_id")!=null&&token.getClaimAsString("client_id")!=null;return valid?OAuth2TokenValidatorResult.success():OAuth2TokenValidatorResult.failure(new OAuth2Error("invalid_token"));};
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(JwtValidators.createDefaultWithIssuer(issuer),claims));return decoder;
    }
    @Bean SecurityFilterChain runtimeSecurity(HttpSecurity http,Environment environment)throws Exception{
        return http.csrf(AbstractHttpConfigurer::disable).httpBasic(AbstractHttpConfigurer::disable).formLogin(AbstractHttpConfigurer::disable)
                .sessionManagement(s->s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(a->a.requestMatchers("/actuator/health/**","/actuator/health").permitAll()
                        .requestMatchers(HttpMethod.POST,"/v1/notifications/*/replays").hasAuthority("SCOPE_notifications:replay")
                        .requestMatchers(HttpMethod.POST,"/v1/notifications").hasAuthority("SCOPE_notifications:write")
                        .requestMatchers(HttpMethod.GET,"/v1/notifications/**").hasAuthority("SCOPE_notifications:read").anyRequest().denyAll())
                .exceptionHandling(e->e.authenticationEntryPoint((r,s,x)->ApiProblems.write(r,s,401,"UNAUTHORIZED")).accessDeniedHandler((r,s,x)->ApiProblems.write(r,s,403,"FORBIDDEN")))
                .oauth2ResourceServer(o->o.jwt(j->{}).authenticationEntryPoint((r,s,x)->ApiProblems.write(r,s,401,"UNAUTHORIZED")))
                .addFilterAfter(new EnvelopeLimitFilter(environment.getProperty("notification.api.maximum-in-flight",Integer.class,512)),org.springframework.security.web.access.intercept.AuthorizationFilter.class).build();
    }
}
