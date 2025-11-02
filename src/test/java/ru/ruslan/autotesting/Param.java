package ru.ruslan.autotesting;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class Param {

    private final Environment environment;

    @Autowired
    public Param(Environment environment) {
        this.environment = environment;
    }

    public String getProperty(String propertyName) {
        return environment.getProperty(propertyName);
    }

    public String getEnv(String variableName) {
        return System.getenv(variableName);
    }
}