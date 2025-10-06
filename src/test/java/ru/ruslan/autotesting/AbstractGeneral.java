package ru.ruslan.autotesting;

// и не использовать аннотации, оставим класс абстрактным

import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractGeneral {

//    public static void printSomeConfigKey() {
//        String value = Param.getProperty("some.config.key3");
//        System.out.println("some.config.key3: " + value);
//    }
//
//    public void printEnvLC_TIME() {
//        String env = Param.getEnv("LC_TIME");
//        System.out.println("LC_TIME: " + env);
//    }
}