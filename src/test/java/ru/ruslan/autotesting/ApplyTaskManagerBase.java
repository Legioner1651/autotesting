package ru.ruslan.autotesting;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.beans.factory.annotation.Autowired;

public class ApplyTaskManagerBase extends AbstractGeneral {

    @Autowired
    private Param param;

    @BeforeAll
    public void beforeAll() {
        System.out.println("----- *** Код метода beforeAll() начал исполняться *** -----");
        String value = param.getProperty("some.config.key2");
        System.out.println("some.config.key2: " + value);
        String env = param.getEnv("LC_MONETARY");
        System.out.println("LC_MONETARY: " + env);
        System.out.println("----- *** Код метода beforeAll() завершился *** -----");
    }


    public void printSomeConfigKey2() {
        String value = param.getProperty("some.config.key2");
        System.out.println("some.config.key2: " + value);
    }

    public void printEnvLC_MONETARY() {
        String env = param.getEnv("LC_MONETARY");
        System.out.println("LC_MONETARY: " + env);
    }
}