package ru.ruslan.autotesting;

import org.junit.jupiter.api.*;

public class ApplyTaskManagerTest {

    @Test
    void testPropertiesAndEnvironment() {
        // Используем статический метод для получения свойства из файла настроек
        String ymlValue = Param.getProperty("some.config.key1");
        System.out.println("Значение из YML: " + ymlValue);

        // Используем статический метод для получения переменной окружения
        String sshAuthSock = Param.getEnv("USERNAME");
        System.out.println("Переменная окружения SSH_AUTH_SOCK: " + sshAuthSock);
    }
}