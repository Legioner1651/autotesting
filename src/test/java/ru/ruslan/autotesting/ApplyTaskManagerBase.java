package ru.ruslan.autotesting;

public class ApplyTaskManagerBase {

    public void printSomeConfigKey2() {
        String value = Param.getProperty("some.config.key2");
        System.out.println("some.config.key2: " + value);
    }

    public void printEnvLC_MONETARY() {
        String env = Param.getEnv("LC_MONETARY");
        System.out.println("LC_MONETARY: " + env);
    }
}