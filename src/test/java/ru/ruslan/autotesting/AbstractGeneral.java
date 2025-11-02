package ru.ruslan.autotesting;

// и не использовать аннотации, оставим класс абстрактным

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractGeneral {

    protected static void writeText2File(String text, Path path) throws IOException {
        Files.writeString(Path.of(path.toUri()), text);
        log.info("writeText2File: Файл успешно создан: " + path + "\"");
    }

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