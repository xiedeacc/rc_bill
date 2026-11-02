package com.rcbill.notification.worker;

import com.rcbill.notification.adapters.scaffold.ScaffoldConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

/** No scheduler or delivery adapter is enabled in the scaffold. */
@SpringBootApplication
@Import(ScaffoldConfiguration.class)
public class NotificationWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationWorkerApplication.class, args);
    }
}
