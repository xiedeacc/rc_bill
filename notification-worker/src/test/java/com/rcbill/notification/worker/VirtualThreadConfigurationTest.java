package com.rcbill.notification.worker;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.task.TaskExecutionAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.AsyncTaskExecutor;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualThreadConfigurationTest {
    @Test
    void applicationConfigurationExecutesTasksOnVirtualThreads() {
        var yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yaml"));
        new ApplicationContextRunner()
                .withInitializer(context -> context.getEnvironment().getPropertySources()
                        .addLast(new PropertiesPropertySource("application-yaml", yaml.getObject())))
                .withConfiguration(AutoConfigurations.of(TaskExecutionAutoConfiguration.class))
                .run(context -> {
                    var executor = context.getBean("applicationTaskExecutor", AsyncTaskExecutor.class);
                    assertThat(executor.submit(() -> Thread.currentThread().isVirtual())
                            .get(5, TimeUnit.SECONDS)).isTrue();
                });
    }
}
