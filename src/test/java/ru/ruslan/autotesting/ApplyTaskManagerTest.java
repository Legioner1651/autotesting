package ru.ruslan.autotesting;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class ApplyTaskManagerTest extends ApplyTaskManagerBase {

    @Autowired
    private Param param;

    String value_45;

    @BeforeAll
    public void beforeAll() {
        System.out.println("===== ----- *** Код метода beforeAll() начал исполняться *** ----- =====");
        value_45 = param.getProperty("some.config.key4");
        System.out.println("some.config.key4: " + value_45);
        System.out.println("===== ----- *** Код метода beforeAll() завершился *** ----- =====");
    }

    @BeforeEach
    public void beforeEach() {
        System.out.println("===== ----- *** Код метода beforeEach() начал исполняться *** ----- =====");
        String value = param.getProperty("some.config.key3");
        System.out.println("some.config.key2: " + value);
        String env = param.getEnv("LC_MONETARY");
        System.out.println("LC_MONETARY: " + env);
        System.out.println("some.config.key4: " + value_45);
        System.out.println("===== ----- *** Код метода beforeEach() завершился *** ----- =====");
    }

    @Test
    void testPropertiesAndEnvironment1() {
        System.out.println("===== Start @Test 1 =====");
        // Используем статический метод для получения свойства из файла настроек
        String ymlValue = param.getProperty("some.config.key1");
        System.out.println("Значение из YML: " + ymlValue);

        // Используем статический метод для получения переменной окружения
        String sshAuthSock = param.getEnv("USERNAME");
        System.out.println("Переменная окружения USERNAME: " + sshAuthSock);
        System.out.println("some.config.key4: " + value_45);
        System.out.println("===== End @Test 1 =====");
    }

    @Test
    void testPropertiesAndEnvironment2() {
        System.out.println("===== Start @Test 2 =====");
        // Используем статический метод для получения свойства из файла настроек
        String ymlValue = param.getProperty("some.config.key1");
        System.out.println("Значение из YML: " + ymlValue);

        // Используем статический метод для получения переменной окружения
        String sshAuthSock = param.getEnv("USERNAME");
        System.out.println("Переменная окружения USERNAME: " + sshAuthSock);
        System.out.println("some.config.key4: " + value_45);
        System.out.println("===== End @Test 2 =====");
    }
}