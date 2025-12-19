package ru.sberbank.sberuser.surms.test.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.qameta.allure.Owner;
import io.qameta.allure.*;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.builder.ResponseSpecBuilder;
import io.restassured.config.LogConfig;
import io.restassured.config.RestAssuredConfig;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.JsonPath;
import io.restassured.specification.RequestSpecification;
import io.restassured.specification.ResponseSpecification;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;
import ru.sberbank.sberuser.surms.test.integration.base.Utilities;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

@Slf4j
@Owner("Ruslan Petrov")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SpringBootTest(classes = AppConfig.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractInstance {

    protected LocalTime timeStart;
    protected LocalTime timeEnd;
    protected String baseUrl = null;

    protected RequestSpecification requestSpecification = new RequestSpecBuilder()
            .setContentType("application/scim+json; charset=utf-8")
            .build();

    protected ResponseSpecification responseSpecification = new ResponseSpecBuilder()
            .build();

    protected RestAssuredConfig restAssuredConfig = RestAssured.config()
            .logConfig(LogConfig.logConfig().enableLoggingOfRequestAndResponseIfValidationFails(LogDetail.ALL));

    @Step("Старт теста {timeStart}")
    protected void headingOfTestMethod(TestInfo testInfo, LocalTime timeStart) {

        System.out.println("\n***** The start of test: " + timeStart + " *****");
        System.out.println("DisplayName = \"" + testInfo.getDisplayName() + "\":");

        if (testInfo.getTestMethod().isPresent()) {
            System.out.println("Method Name = \"" + testInfo.getTestMethod().get().getName() + "\":");
        }
    }

    @Step("Окончание теста {timeEnd}")
    protected void endOfTestMethod(LocalTime timeStart, LocalTime timeEnd) {

        System.out.println("***** The end of test: " + timeEnd + ", Total time: "
                + Duration.between(timeStart, timeEnd).getSeconds() + ","
                + Duration.between(timeStart, timeEnd).getNano() + " seconds *****\n");
    }

    @Step("Топик {topic}: Проверка полученного сообщения из kafka с ожидаемым результатом")
    public <K, V> void checkGetMessageFromKafka(JsonNode expected, List<Map.Entry<K, V>> listMapActual, String topic) throws JsonProcessingException {
        String message = "ERROR:  Проверка сообщений из топика " + topic + " с ожидаемым результатом";

        /* Перекладываем данные в Map */
        HashMap<K, V> map = new HashMap<>();
        for(Map.Entry<K, V> entry : listMapActual){
            map.put(entry.getKey(), entry.getValue());
        }

        /* Создаём экземпляр объекта для работы с JSON */
        ObjectMapper mapper = new ObjectMapper();

        /* Конвертируем Map в JsonNode */
        JsonNode jsonNodeActual = mapper.valueToTree(map);
        Assertions.assertEquals(expected, mapper.readTree(String.valueOf(jsonNodeActual)), message);
    }

    @Step("Топик {topic}: Проверка полученного сообщения из kafka с ожидаемым результатом")
    public <K, V> void checkGetMessageFromKafka(JsonNode expected, JsonNode received, String topic) throws JsonProcessingException {

        String message = "ERROR:  Проверка сообщений из топика " + topic + " с ожидаемым результатом";
        Assertions.assertEquals(expected, received, message);
    }

    @Step("Топик {topic}: Проверка изменений в БД после обработки сообщения из kafka")
    public void checkChangesInDatabase(String expected, String actual, String topic) {
        String message = "ERROR: Данные в БД |" + actual + "| не соответствует ожидаемым |" + expected + "|";
        Assertions.assertEquals(expected, actual, message);
    }

    @Step("Установка разметки ЦОИФТ для сервиса SURMS.apply.taskManager")
    protected void setAllureLabelsATM() {
        Allure.label("system", "CI03710534");
        Allure.label("team", "Ролевые модели [62868715]");
        Allure.label("story", "SURMS-0000");
        Allure.label("layer", "api");
        Allure.label("testStage", "ift");
        Allure.label("criticalRegress", "true");
        Allure.label("regress", "true");
    }

    @Step("Установка разметки ЦОИФТ для сервиса SURMS.sudirScimProvider")
    protected void setAllureLabelsSSP() {
        Allure.label("system", "CI04123162");
        Allure.label("team", "Ролевые модели [62868715]");
        Allure.label("story", "SURMS-0000");
        Allure.label("layer", "api");
        Allure.label("testStage", "ift");
        Allure.label("criticalRegress", "true");
        Allure.label("regress", "true");
    }

    @Step("Чтение файла из папки sources: {path} и преобразование в строку")
    protected static String readFileFromResources(String filePath) {
        // Получаем InputStream для файла из ресурсов
        InputStream inputStream = Utilities.class.getClassLoader().getResourceAsStream(filePath);

        // Проверяем, что файл был найден
        if (inputStream == null) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }

        // Используем Scanner для чтения файла
        Scanner scanner = new Scanner(inputStream, "UTF-8");
        scanner.useDelimiter("\\A");
        String fileContent = scanner.hasNext() ? scanner.next() : "";

        // Закрываем Scanner
        scanner.close();

        return fileContent;
    }

    @Step("Чтение файла по пути {path}")
    protected static String readFile(String path) throws IOException {
        return new String(Files.readAllBytes(Paths.get(path)));
    }

    @Step("Создание файла по пути {path}")
    protected static void writeText2File(String text, Path path) {
        try {
            Files.writeString(Path.of(path.toUri()), text);
            log.info("writeText2File: Файл успешно создан: " + path +"\"");
        } catch (IOException e) {
            log.error("Ошибка записи в файл: " + e.getMessage());
            throw new RuntimeException("Ошибка записи файла: " + e.getMessage(), e);
        }
    }

    /** * Метод добавления файла в отчёт Allure */
    @Attachment(value = "{attachmentName}", type = "text/plain")
    protected static byte[] attachFile(String attachmentName, Path path) throws IOException {
        return Files.readAllBytes(path);
    }

    @Step("Получение расположения JSON файла с ожидаемым значением")
    protected JsonPath getJsonPath(String pathname) {
        return new JsonPath(new File(pathname));
    }


    @Step("Для информации: Вывод в лог полученные сообщения")
    protected <K, V> void loggingReceivedMessagesFromListMapEntry(List<Map.Entry<K, V>> listMapEntry) {
        listMapEntry.forEach(entry -> { log.info("Ключ: " + entry.getKey() + ", Значение: " + entry.getValue()); });
    }
}
