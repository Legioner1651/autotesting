package ru.ruslan.autotesting;

import org.junit.jupiter.api.Test; // не обязательно, т.к. без аннотаций
// и не использовать аннотации, оставим класс абстрактным

public abstract class GeneralMethods {

    public static void printSomeConfigKey() {
        String value = Param.getProperty("some.config.key3");
        System.out.println("some.config.key3: " + value);
    }

    public void printEnvLC_TIME() {
        String env = Param.getEnv("LC_TIME");
        System.out.println("LC_TIME: " + env);
    }
}