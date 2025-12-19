package taskmanager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.qameta.allure.Feature;
import io.qameta.allure.Link;
import io.qameta.allure.Step;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import ru.sberbank.sberuser.surms.test.integration.AbstractInstance;
import ru.sberbank.sberuser.surms.test.integration.JdbcService;
import ru.sberbank.sberuser.surms.test.integration.Param;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

/* Источник получения переменных окружения */
import static ru.sberbank.sberuser.surms.test.integration.ConfigEnv.*;

@Slf4j
@Feature("Сервис SURMS.apply.taskManager")
@Link(name = "Issue", url = "https://jira.delta.sbrf.ru/browse/SURMS-4253")
@Testcontainers
public class ApplyTaskManagerBase extends AbstractInstance {

    /* Источник получения параметров из application.yml */
    @Autowired
    protected Param param;

    protected static final String ALIAS_CONTAINER_POSTGRES = "postgresAliase";
    protected static final String SURMS_DS_URL = String.format("jdbc:postgresql://%s/surms", ALIAS_CONTAINER_POSTGRES);
    protected static final String ALIAS_CONTAINER_KAFKA = "kafkaAliase";
    protected static final Network network = Network.newNetwork();
    protected static final ZoneId localTimeZone = ZoneId.systemDefault();
    protected String bootstrapServersKafka;
    protected JdbcService jdbcService;
    protected String testMethod;
    protected String topicName;
    protected String groupId;
    protected Properties kafkaProps;

    protected String surmsDsUser;
    protected String surmsDsPassword;
    protected String surmsDsOwnerUser;
    protected String surmsDsOwnerPassword;

    protected static Logger logTest = LoggerFactory.getLogger(ApplyTaskManagerTest.class);
    protected static Logger logBase = LoggerFactory.getLogger(ApplyTaskManagerBase.class);

    /* Используется для вывода логов контейнеров в консоль */
    protected static Slf4jLogConsumer logPostgresConsumer = new Slf4jLogConsumer(LoggerFactory.getLogger("postgres-service"));
    protected static Slf4jLogConsumer logKafkaConsumer = new Slf4jLogConsumer(LoggerFactory.getLogger("kafka-service"));
    protected static Slf4jLogConsumer logApplyTaskManagerConsumer = new Slf4jLogConsumer(LoggerFactory.getLogger("applyTaskManager-service"));

    protected static KafkaContainer containerKafka = new KafkaContainer(KAFKA_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(ALIAS_CONTAINER_KAFKA)
//            .withLogConsumer(logKafkaConsumer)
            .withEnv("TZ", localTimeZone.toString())
            .withEnv("KAFKA_LOG4J_LOGGERS", "kafka=DEBUG")
            .withStartupTimeout(Duration.ofMinutes(5));

    protected PostgreSQLContainer<?> containerPostgreSQL;
    protected GenericContainer<?> containerApplyTaskManager;

    @BeforeAll
    protected void beforeAll() {
        log.info("\n*********** The start of beforeAll **********");

        surmsDsUser = param.getProperty("spring.datasource.username");
        surmsDsPassword = param.getProperty("spring.datasource.password");
        surmsDsOwnerUser = param.getProperty("spring.liquibase.parameters.username");
        surmsDsOwnerPassword = param.getProperty("spring.liquibase.parameters.password");

        log.info("************ The end of beforeAll ***********");
    }

    @BeforeEach
    protected void beforeEach(TestInfo testInfo) {
        log.info("************ The start of @BeforeEach ************\n");

        timeStart= LocalTime.now();
        headingOfTestMethod(testInfo, timeStart);

        if (testInfo.getTestMethod().isPresent()) {
            testMethod = testInfo.getTestMethod().get().getName();
        }

        log.info("Start of containerKafka");
        containerKafka.start();
        log.info("containerKafka is already started");

        bootstrapServersKafka = containerKafka.getBootstrapServers();
        log.info("bootstrapServersKafka = \"{}\"", bootstrapServersKafka);
        log.info("localTimeZone         = \"{}\"", localTimeZone);

        /* Инициализация свойств клиента Kafka */
        kafkaProps = new Properties();
        kafkaProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);

        /* Создание топиков в Kafka */
        try (AdminClient adminClient = AdminClient.create(kafkaProps)) {
            Collection<NewTopic> topics =  param.getTopicNamesFromYml();
            adminClient.createTopics(topics);
            log.info("Топики успешно созданы!");
            log.info("topics.size() = {}", topics.size());
        }

        containerPostgreSQL = new PostgreSQLContainer<>(SURMS_DS_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(ALIAS_CONTAINER_POSTGRES)
                .withExposedPorts(5432)
                .withEnv("TZ", localTimeZone.toString())
                .withDatabaseName("surms")
//                .withLogConsumer(logPostgresConsumer)
                .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 1))
                .withStartupTimeout(Duration.ofMinutes(5));

        log.info("Start of containerPostgreSQL");
        containerPostgreSQL.start();
        log.info("The containerPostgreSQL is already started");

        containerApplyTaskManager = new GenericContainer<>(APPLY_TASK_MANAGER_IMAGE)
                .withNetwork(network)
                .dependsOn(containerPostgreSQL, containerKafka)
//                .withLogConsumer(logApplyTaskManagerConsumer)
                .withEnv("SURMS_SERVER_PORT", "8080")
                .withEnv("SURMS_MANAGEMENT_SERVER_PORT", "8090")
                .withEnv("SURMS_DS_URL", SURMS_DS_URL)
                .withEnv("SURMS_DS_DRIVER", "org.postgresql.Driver")
                .withEnv("SURMS_DS_USER", surmsDsUser)
                .withEnv("SURMS_DS_PASSWORD", surmsDsPassword)
                .withEnv("SURMS_DS_OWNER_USER", surmsDsOwnerUser)
                .withEnv("SURMS_DS_OWNER_PASSWORD", surmsDsOwnerPassword)
                .withEnv("SURMS_KAFKA_BOOTSTRAP_SERVERS", ALIAS_CONTAINER_KAFKA + ":9092")
                .withEnv("LOGGING_LEVEL_RU", "debug")
                .withEnv("TZ", localTimeZone.toString())
                .withEnv("APP_TIMING_EQ_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_EQ_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_EQ_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQSUDIR_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQSUDIR_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_ESQSUDIR_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_APPLYNOTIFICATION_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_APPLYNOTIFICATION_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_APPLYNOTIFICATION_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQESRT_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQESRT_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_ESQESRT_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_POSTAPPLY_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_POSTAPPLY_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_POSTAPPLY_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_CREATEZNO_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_CREATEZNO_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_CREATEZNO_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_CREATESD_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_CREATESD_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_CREATESD_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_COMPLETEZNO_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_COMPLETEZNO_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_COMPLETEZNO_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_DENYZNO_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_DENYZNO_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_DENYZNO_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQSM_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQSM_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_ESQSM_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQAD_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQAD_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_ESQAD_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQIDM_POLLINGINTERVAL", "PT1S")
                .withEnv("APP_TIMING_ESQIDM_CACHEEXPIREDURATION", "PT10M")
                .withEnv("APP_TIMING_ESQIDM_CACHEEXPIRECHECKINTERVAL", "PT1S")
                .withCommand("/opt/app/scripts/docker-entrypoint.sh")
                .waitingFor(Wait.forLogMessage(".*Polling EMPLOYEE_SERVICE_QUEUE.*\\s", 1))
                .withStartupTimeout(Duration.ofMinutes(5));

        log.info("Start of containerApplyTaskManager");
        containerApplyTaskManager.start();
        log.info("The containerApplyTaskManager is already started");

        /* Вывод в консоль логов контейнеров после выполнения команды */
//        containerKafka.followOutput(logKafkaConsumer);                          // kafka-service
//        containerPostgreSQL.followOutput(logPostgresConsumer);                  // postgres-service
//        containerApplyTaskManager.followOutput(logApplyTaskManagerConsumer);    // applyTaskManager-service

        /* Получение настройки dbUrl для подключения к БД */
        String dbUrl = String.format("jdbc:postgresql://%s:%d/surms", containerPostgreSQL.getHost(), containerPostgreSQL.getFirstMappedPort());

        jdbcService = new JdbcService(dbUrl, surmsDsUser, surmsDsPassword);

        /* Визуализация URL для подключения к БД в тестовом методе */
        log.info("JdbcService.dbUrl                  = \"{}\"", dbUrl);

        log.info("************ The end of @BeforeEach ************\n");
    }

    @AfterEach
    protected void afterEach() throws IOException {
        log.debug("");
        log.debug("************ The start of @AfterEach ************");

        /* Создание файлов с логами контейнеров после каждого теста, если logger = DEBUG ИЛИ TRACE */
        if (logBase.isDebugEnabled() || logTest.isDebugEnabled()) {

            String fileName;

            fileName = String.format("containerPostgreSQL-%s.log", testMethod);
            writeText2File(containerPostgreSQL.getLogs(), Paths.get("target/" + fileName));
            attachFile(fileName, Paths.get("target/" + fileName));

            fileName = String.format("containerKafka-%s.log", testMethod);
            writeText2File(containerKafka.getLogs(), Paths.get("target/" + fileName));
            attachFile(fileName, Paths.get("target/" + fileName));

            fileName = String.format("containerApplyTaskManager-%s.log", testMethod);
            writeText2File(containerApplyTaskManager.getLogs(), Paths.get("target/" + fileName));
            attachFile(fileName, Paths.get("target/" + fileName));
        }

        containerApplyTaskManager.stop();
        containerPostgreSQL.stop();
        containerKafka.stop();

        timeEnd= LocalTime.now();
        endOfTestMethod(timeStart, timeEnd);
    }

    @AfterAll
    protected void afterAll() {
        log.info("");
        log.info("*********** The start of afterAll **********");
        log.info("************ The end of afterAll ***********\n");
    }

    @Step("Преобразование Instant {timeNow} в Timestamp")
    public String instant2Timestamp(Instant timeNow) {
        log.debug("********** Start instant2timestamp() **********");
        log.debug("timeNow.toString() = {}", timeNow.toString());

        /* Создаем шаблон для вывода в нужном формате */
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

        /* Преобразуем Instant в LocalDateTime для удобного округления */
        LocalDateTime ldt = LocalDateTime.ofInstant(timeNow, localTimeZone);

        /* Округляем до ближайших микросекунд (шесть знаков после запятой) */
        log.debug("ldt.getNano() = {}", ldt.getNano());
        long millisRounded = Math.round((ldt.getNano() / 1_000d)) * 1_000L;
        ldt = ldt.withNano((int) millisRounded);

        /* Форматируем локальное время и выводим */
        String responseSmTimestamp = ldt.format(formatter);

        /* Убираем в строке крайние справа нули */
        responseSmTimestamp = responseSmTimestamp.replaceAll("0+$", "");
        log.debug("responseSmTimestamp = {}", responseSmTimestamp);
        log.debug("********** End instant2timestamp() **********");
        return responseSmTimestamp;
    }

    @Step("Преобразование LocalDateTime {timeNow} в Timestamp")
    public String localDateTime2Timestamp(LocalDateTime timeNow) {
        log.debug("********** Start instant2timestamp() **********");
        log.debug("timeNow.toString() = {}", timeNow.toString());

        /* Создаем шаблон для вывода в нужном формате */
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

        /* Преобразуем Instant в LocalDateTime для удобного округления */
        LocalDateTime ldt = timeNow;

        /* Округляем до ближайших микросекунд (шесть знаков после запятой) */
        log.debug("ldt.getNano() = {}", ldt.getNano());
        long millisRounded = Math.round((ldt.getNano() / 1_000d)) * 1_000L;
        ldt = ldt.withNano((int) millisRounded);

        /* Форматируем локальное время и выводим */
        String responseSmTimestamp = ldt.format(formatter);

        /* Убираем в строке крайние справа нули */
        responseSmTimestamp = responseSmTimestamp.replaceAll("0+$", "");
        log.debug("responseSmTimestamp = {}", responseSmTimestamp);
        log.debug("********** End instant2timestamp() **********");
        return responseSmTimestamp;
    }

    @Step("Конвертация списка Map.Entry в JSON-объект")
    public static JsonNode convertListMapEntryToJsonNode(List<Map.Entry<Long, String>> entries) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode result = mapper.createObjectNode();

        for (Map.Entry<Long, String> entry : entries) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.set(entry.getKey().toString(), mapper.readTree(entry.getValue()));
            }
        }

        return result;
    }
}
