package ru.ruslan.autotesting;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

@Configuration
@EnableAutoConfiguration
@TestPropertySource(locations="classpath:/application.yml") // путь к вашему файлу конфигурации
public class TestConfig {}
