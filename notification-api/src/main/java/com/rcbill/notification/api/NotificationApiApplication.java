package com.rcbill.notification.api;

import com.rcbill.notification.adapters.scaffold.ScaffoldConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

@SpringBootApplication
@Import({ScaffoldConfiguration.class,com.rcbill.notification.adapters.config.StartupConfiguration.class,com.rcbill.notification.adapters.config.PersistenceConfiguration.class})
public class NotificationApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationApiApplication.class, args);
    }
}
