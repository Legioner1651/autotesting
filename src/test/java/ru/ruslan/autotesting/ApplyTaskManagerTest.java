package ru.ruslan.autotesting;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ApplyTaskManagerTest extends ApplyTaskManagerBase {

    @Autowired
    private Param param;

    @BeforeEach
    public void beforeEach() {
        System.out.println("----- *** Код метода beforeEach() начал исполняться *** -----");
        String value = param.getProperty("some.config.key3");
        System.out.println("some.config.key2: " + value);
        String env = param.getEnv("LC_MONETARY");
        System.out.println("LC_MONETARY: " + env);
        System.out.println("----- *** Код метода beforeEach() завершился *** -----");
    }

    @Test
    void testPropertiesAndEnvironment() {
        System.out.println("===== Start @Test =====");
        // Используем статический метод для получения свойства из файла настроек
        String ymlValue = param.getProperty("some.config.key1");
        System.out.println("Значение из YML: " + ymlValue);

        // Используем статический метод для получения переменной окружения
        String sshAuthSock = param.getEnv("USERNAME");
        System.out.println("Переменная окружения USERNAME: " + sshAuthSock);
        System.out.println("===== End @Test =====");
    }
}