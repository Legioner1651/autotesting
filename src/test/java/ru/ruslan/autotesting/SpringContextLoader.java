package ru.ruslan.autotesting;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.Environment;

public class SpringContextLoader {
    private static ConfigurableApplicationContext context;

    public static synchronized Environment getEnvironment() {
        if (context == null) {
            context = SpringApplication.run(AppConfig.class);
        }
        return context.getEnvironment();
    }
}
