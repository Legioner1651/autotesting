package ru.ruslan.autotesting;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class Param {

    private static volatile Environment environment;

    @Autowired
    public void setEnvironment(Environment env) {
        if (environment == null) { // Инициализация однократно
            synchronized (this.getClass()) {
                if (environment == null)
                    environment = env;
            }
        }
    }

    /**
     * Получение значения параметра из конфигурационного файла application.yml
     */
    public static String getProperty(String propertyName) {
//        return environment.getProperty(propertyName);
        return "--- Заглушка для метода getProperty() ---";
    }

    /**
     * Получение значения переменной окружения по её имени
     */
    public static String getEnv(String variableName) {
        return System.getenv(variableName); // Доступ к переменным окружения
    }
}