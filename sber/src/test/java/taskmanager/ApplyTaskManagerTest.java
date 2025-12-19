package taskmanager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.qameta.allure.AllureId;
import io.qameta.allure.Description;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.KafkaJsonSerializer;
import ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.domain.*;
import ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.producer.MessageSender;
import ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.domain.CompleteZNOResponse;
import ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.domain.CreateSDResponse;
import ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.domain.CreateZNOResponse;

import java.io.File;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.*;
import static io.qameta.allure.Allure.step;
import static ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.consumer.KafkaConsumerService.consumeAllRecords;

@Slf4j
public class ApplyTaskManagerTest extends ApplyTaskManagerBase {

    @Test
    @Order(value = 1)
    @AllureId("5476125")
    @DisplayName("ATM 01. EqTaskProvider")
    @Tags({@Tag("kafka"), @Tag("eq"), @Tag("ATM.01")})
    @Description("Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.employee_queue, формирует и отправляет сообщение в топик topics.eq-out.")
    void testEqTaskProvider() throws SQLException, JsonProcessingException {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.eq-out", step -> {});
        log.info("Этап I: Отправка сообщения в topics.eq-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.eq-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.eq-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = "INSERT INTO surms.employee_queue (employee_id,processed,source_id,is_locked,check_after,result_status_id) VALUES (455715,false,28,false,NULL,1)";

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, Long> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, Long>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Ожидаемые сообщения из топики */
        String response = "{ \"455715\" : 5906757 }";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(response);

        /* Проверка сообщений из топика employee-queue-tasks с ожидаемым результатом */
        checkGetMessageFromKafka(expectedJsonNode, allRecords, topicName);
    }

    @Test
    @Order(value = 2)
    @AllureId("6475016")
    @DisplayName("ATM 02. EsqSudirTaskProvider")
    @Tags({@Tag("kafka"), @Tag("esq-sudir"), @Tag("ATM.02")})
    @Description("Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.employee_service_queue, формирует и отправляет сообщение в топик topics.esq-sudir-out.")
    void testEsqSudirTaskProvider() throws SQLException, JsonProcessingException {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.esq-sudir-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.esq-sudir-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.esq-sudir-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.esq-sudir-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = "INSERT INTO surms.employee_queue (employee_id,processed,source_id,is_locked,check_after,result_status_id) VALUES (455715,true,28,false,NULL,1)";
        String sqlInsert2 = readFileFromResources("applytaskmanager/postgres/sql/initialization/02.employee-service-queue-sudir-tasks.sql");

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, Long> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, Long>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));
            log.info("jdbcService.callDbInsert(sqlInsert2) = \"{}\"", jdbcService.callDbInsert(sqlInsert2));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Ожидаемые сообщения из топики */
        String response = "{ \"5906757\" : 236218 }";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(response);

        /* Проверка сообщений из топика employee-service-queue-sudir-tasks с ожидаемым результатом */
        checkGetMessageFromKafka(expectedJsonNode, allRecords, topicName);
    }

    @Test
    @Order(value = 3)
    @AllureId("5476295")
    @DisplayName("ATM 03. ApplyNotificationTaskProvider")
    @Tags({@Tag("kafka"), @Tag("apply-notification"), @Tag("ATM.03")})
    @Description("Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления связанных записей в таблицах employee_queue, done_status, employee_service_queue, sm_queue, sudir_queue, формирует и отправляет сообщение в топик topics.apply-notification-out.")
    void testApplyNotificationTaskProvider() throws SQLException, JsonProcessingException {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.apply-notification-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.apply-notification-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.apply-notification-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.apply-notification-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = "INSERT INTO surms.employee_queue (employee_id,processed,source_id,is_locked,check_after,result_status_id) VALUES (455715,true,28,false,NULL,1)";
        String sqlInsert2 = "INSERT INTO surms.employee_service_queue (employee_queue_id,processed,result_status_id,result_comment,base_service_id,action_id) " +
                "VALUES ((select min(id) from surms.employee_queue where result_status_id = 1),true,1,'result_comment',142,1)";
        String sqlInsert3 = "INSERT INTO surms.sudir_queue (employee_service_queue_id,processed,send_status_id) VALUES (236218,true,1)";
        String sqlInsert4 = "INSERT INTO surms.sm_queue (processed,employee_service_queue_id,contact_name,callback_contact,id_profile) VALUES (true,236218,'name','callback','profile')";

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, Long> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, Long>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));
            log.info("jdbcService.callDbInsert(sqlInsert2) = \"{}\"", jdbcService.callDbInsert(sqlInsert2));
            log.info("jdbcService.callDbInsert(sqlInsert3) = \"{}\"", jdbcService.callDbInsert(sqlInsert3));
            log.info("jdbcService.callDbInsert(sqlInsert4) = \"{}\"", jdbcService.callDbInsert(sqlInsert4));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Ожидаемые сообщения из топики */
        String response = "{ \"455715\" : 5906757 }";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(response);

        /* Проверка сообщений из топика apply-notification-tasks с ожидаемым результатом */
        checkGetMessageFromKafka(expectedJsonNode, allRecords, topicName);
    }

    @Test
    @Order(value = 4)
    @AllureId("6591266")
    @DisplayName("ATM 04. EsqEsrtTaskProvider")
    @Tags({@Tag("kafka"), @Tag("esq-esrt"), @Tag("ATM.04")})
    @Description("Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления связанных записей в таблицах employee_service_queue, employee_queue, source_label, формирует и отправляет сообщение в топик topics.esq-esrt-out.")
    void testEsqEsrtTaskProvider() throws SQLException, JsonProcessingException {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.esq-esrt-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.esq-esrt-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.esq-esrt-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.esq-esrt-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = "INSERT INTO surms.employee_queue (employee_id,processed,source_id,is_locked,check_after,result_status_id) VALUES (455715,true,28,false,NULL,1)";
        String sqlInsert2 = "INSERT INTO surms.employee_service_queue (employee_queue_id,base_service_id,check_after,processed,result_status_id,result_comment,action_id,\"data\",processed_date,target) " +
                "VALUES ((select min(id) from surms.employee_queue where result_status_id = 1),142,'2025-03-24 08:54:50.125',false,1,'result_comment',1,'{\"serviceId\":35130}','2025-03-24 08:53:50.166','SM_ESRT')";

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, Long> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, Long>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));
            log.info("jdbcService.callDbInsert(sqlInsert2) = \"{}\"", jdbcService.callDbInsert(sqlInsert2));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Ожидаемые сообщения из топики */
        String response = "{ \"5906757\" : 236218 }";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(response);

        /* Проверка сообщений из топика employee-service-queue-sm-esrt-tasks с ожидаемым результатом */
        checkGetMessageFromKafka(expectedJsonNode, allRecords, topicName);
    }

    @Test
    @Order(value = 5)
    @AllureId("6591267")
    @DisplayName("ATM 05. PostApplyTaskProvider")
    @Tags({@Tag("kafka"), @Tag("post-apply"), @Tag("ATM.05")})
    @Description("Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.employee_post_apply_queue, формирует и отправляет сообщение в топик topics.post-apply-out.")
    void testPostApplyTaskProvider() throws SQLException, JsonProcessingException {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.post-apply-out", step -> {});
        log.info("Этап I: Отправка сообщения в topics.post-apply-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.post-apply-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.post-apply-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = readFileFromResources("applytaskmanager/postgres/sql/initialization/05.employee-post-apply-tasks.sql");

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, Long> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, Long>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Ожидаемые сообщения из топики */
        String response = "{ \"455715\" : 97 }";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(response);

        /* Проверка сообщений из топика employee-post-apply-tasks с ожидаемым результатом */
        checkGetMessageFromKafka(expectedJsonNode, allRecords, topicName);
    }

    @Test
    @Order(value = 6)
    @AllureId("6591268")
    @DisplayName("ATM 06. CreateZNOTaskProvider")
    @Tags({@Tag("kafka"), @Tag("create-zno"), @Tag("ATM.06")})
    @Description(
            "Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.surms.sm_queue_agreement, формирует и отправляет сообщение в топик topics.create-zno-out.\n"
                    + "Этап II: проверяет, что после получения сообщения из топика topics.create-zno-in сервис surms-apply-taskmanager вносит изменения в определенные поля родительской записи таблицы surms.sm_queue_agreement"
    )
    void testCreateZNOTaskProvider() throws Exception {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.create-zno-out", step -> {});
        log.info("Этап I: Отправка сообщения в topics.create-zno-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.create-zno-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.create-zno-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String description = readFileFromResources("applytaskmanager/postgres/sql/initialization/06.service-manager-zno-create-tasks-description.txt");
        String sqlInsert1 = readFileFromResources("applytaskmanager/postgres/sql/initialization/06.service-manager-zno-create-tasks.sql");
        sqlInsert1 = String.format(sqlInsert1, description);

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, String>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Преобразование полученных ключей и сообщений в JsonNode */
        JsonNode receivedJsonNode = convertListMapEntryToJsonNode(allRecords);

        /* Ожидаемые сообщения из топики */
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(new File("src/test/resources/applytaskmanager/kafka/messages/expected/service-manager-zno-create-tasks.json"));

        /* Проверка сообщений из топика employee-queue-tasks с ожидаемым результатом */
        checkGetMessageFromKafka(expectedJsonNode, receivedJsonNode, topicName);

        step("Этап II: Обработка сообщения из topics.create-zno-in", () -> {});
        log.info("Этап II: Обработка сообщения из topics.create-zno-in:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.create-zno-in");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.create-zno-in");
        System.out.println("groupId = " + groupId + "\n");

        /* Создание объекта для отправки в Kafka */
        Instant timeNow = Instant.now();
        CreateZNOResponse createZNOResponse = new CreateZNOResponse(715, "Успешно", "Запись успешно добавлена", "ЗНО0411450355", timeNow);

        /* Настройка свойств продюсера */
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        MessageSender messageSender = new MessageSender(kafkaProps);

        /* Отправка сообщения в Kafka */
        var send = messageSender.sendMessage(topicName, 1L, createZNOResponse);

        /* Результат отправки сообщения в Kafka */
        log.info("Into topic \"{}\" send message = {}, result = {}", topicName, createZNOResponse, send);

        await().pollDelay(Duration.ofSeconds(3)).timeout(Duration.ofMinutes(1)).until(() -> true);

        /* Значения полей, которые меняет сервис */
        String actual2 = jdbcService.callDbSelect("SELECT response_sm, processed, response_sm_message, processed_retrieve FROM surms.sm_queue_agreement WHERE id = 715");

        /* Ожидаемые значения полей в БД */
        String expected2 = "Успешно, t, Запись успешно добавлена, f\n";

        /* Проверка измененных полей в таблице surms.sm_queue_agreement с ожидаемым результатом */
        checkChangesInDatabase(expected2, actual2, topicName);
    }

    @Test
    @Order(value = 7)
    @AllureId("6453836")
    @DisplayName("ATM 07. CreateSdTaskProvider")
    @Tags({@Tag("kafka"), @Tag("create-sd"), @Tag("ATM.07")})
    @Description(
            "Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.sm_queue связанной с таблицами employee_service_queue, employee_queue, source_label, формирует и отправляет сообщение в топик topics.create-sd-out.\n"
                    + "Этап II: проверяет, что после получения сообщения из топика topics.create-sd-in сервис surms-apply-taskmanager вносит изменения в определенные поля родительской записи таблицы surms.sm_queue"
    )
    void testCreateSdTaskProvider() throws Exception {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.create-sd-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.create-sd-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.create-sd-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.create-sd-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = "INSERT INTO surms.employee_queue (employee_id,processed,source_id,is_locked,check_after,result_status_id) VALUES (455715,false,28,false,NULL,1)";
        String sqlInsert2 = "INSERT INTO surms.employee_service_queue (employee_queue_id,processed,result_status_id,result_comment,base_service_id,action_id) " +
                "VALUES ((select min(id) from surms.employee_queue where result_status_id = 1),false,1,'result_comment',142,1)";
        String sqlInsert3 = "INSERT INTO surms.sudir_queue (employee_service_queue_id,processed,send_status_id) VALUES (236218,false,1)";
        String sqlInsert4 = "INSERT INTO surms.sm_queue (processed,employee_service_queue_id,contact_name,callback_contact,id_profile) VALUES (false,236218,'name','callback','profile')";
        String parameters = readFileFromResources("applytaskmanager/postgres/sql/initialization/07.service-manager-sd-create-tasks-parameters.html");
        String description = readFileFromResources("applytaskmanager/postgres/sql/initialization/07.service-manager-sd-create-tasks-description.txt");
        String sqlInsert5 = readFileFromResources("applytaskmanager/postgres/sql/initialization/07.service-manager-sd-create-tasks.sql");

        sqlInsert5 = String.format(sqlInsert5, description, parameters);

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, String>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));
            log.info("jdbcService.callDbInsert(sqlInsert2) = \"{}\"", jdbcService.callDbInsert(sqlInsert2));
            log.info("jdbcService.callDbInsert(sqlInsert3) = \"{}\"", jdbcService.callDbInsert(sqlInsert3));
            log.info("jdbcService.callDbInsert(sqlInsert4) = \"{}\"", jdbcService.callDbInsert(sqlInsert4));
            log.info("jdbcService.callDbInsert(sqlInsert5) = \"{}\"", jdbcService.callDbInsert(sqlInsert5));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Преобразование полученных ключей и сообщений в JsonNode */
        JsonNode receivedJsonNode = convertListMapEntryToJsonNode(allRecords);

        log.info("\nreceivedJsonNode = {}", receivedJsonNode);

        /* Ожидаемые сообщения из топики */
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(new File("src/test/resources/applytaskmanager/kafka/messages/expected/service-manager-sd-create-tasks.json"));

        /* Проверка сообщений из топика service-manager-sd-create-tasks с ожидаемым результатом */
        log.info("--- Проверка сообщений из топика service-manager-sd-create-tasks с ожидаемым результатом ---");
        checkGetMessageFromKafka(expectedJsonNode, receivedJsonNode, topicName);

        step("Этап II: Обработка сообщения из topics.create-sd-in", () -> {});
        log.info("Этап II: Обработка сообщения из topics.create-sd-in:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.create-sd-in");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.create-sd-in");
        System.out.println("groupId = " + groupId + "\n");

        /* Создание объекта для отправки в Kafka */
        Instant timeNow = Instant.now();
        CreateSDResponse createSDResponse = new CreateSDResponse(1427, "Успешно", "Запись успешно добавлена", "ЗНО0411450355", timeNow);

        /* Настройка свойств продюсера */
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        MessageSender messageSender = new MessageSender(kafkaProps);

        /* Отправка сообщения в Kafka */
        log.info("*** Отправка сообщения в Kafka ***");
        var send = messageSender.sendMessage(topicName, 1L, createSDResponse);

        /* Результат отправки сообщения в Kafka */
        log.info("Into topic \"{}\" send message = {}, result = {}", topicName, createSDResponse, send.getOffset());

        await().pollDelay(Duration.ofSeconds(3)).timeout(Duration.ofMinutes(1)).until(() -> true);

        /* Значения полей, которые меняет сервис */
        String actual2 = jdbcService.callDbSelect("SELECT id, processed, response_sm, response_sm_timestamp, object_id, response_sm_message FROM surms.sm_queue WHERE id = 1427");

        /* Преобразование даты в формат, который используется в PostgreSQL */
        String responseSmTimestamp = instant2Timestamp(timeNow);

        /* Ожидаемые значения полей в БД */
        String expected2 = String.format("1427, t, Успешно, %s, ЗНО0411450355, Запись успешно добавлена\n", responseSmTimestamp);

        /* Проверка измененных полей в таблице surms.sm_queue_agreement с ожидаемым результатом */
        checkChangesInDatabase(expected2, actual2, topicName);
    }

    @Test
    @Order(value = 8)
    @AllureId("6591269")
    @DisplayName("ATM 08. CompleteZNOTaskProvider")
    @Tags({@Tag("kafka"), @Tag("complete-zno"), @Tag("ATM.08")})
    @Description(
            "Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.sm_queue_action, формирует и отправляет сообщение в топик topics.complete-zno-out.\n"
                    + "Этап II: проверяет, что после получения сообщения из топика topics.complete-zno-in сервис surms-apply-taskmanager вносит изменения в определенные поля родительской записи таблицы surms.sm_queue_action"
    )
    void testCompleteZNOTaskProvider() throws Exception {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.complete-zno-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.complete-zno-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.complete-zno-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.complete-zno-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = readFileFromResources("applytaskmanager/postgres/sql/initialization/08.service-manager-zno-complete-tasks.sql");

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, String>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Преобразование полученных ключей и сообщений в JsonNode */
        JsonNode receivedJsonNode = convertListMapEntryToJsonNode(allRecords);

        log.info("\nreceivedJsonNode = {}", receivedJsonNode);

        /* Ожидаемые сообщения из топики */
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(new File("src/test/resources/applytaskmanager/kafka/messages/expected/service-manager-zno-complete-tasks.json"));

        /* Проверка сообщений из топика service-manager-zno-complete-tasks с ожидаемым результатом */
        log.info("--- Проверка сообщений из топика service-manager-sd-create-tasks с ожидаемым результатом ---");
        checkGetMessageFromKafka(expectedJsonNode, receivedJsonNode, topicName);

        step("Этап II: Обработка сообщения из topics.complete-zno-in", () -> {});
        log.info("Этап II: Обработка сообщения из topics.complete-zno-in:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.complete-zno-in");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.complete-zno-in");
        System.out.println("groupId = " + groupId + "\n");

        /* Создание объекта для отправки в Kafka */
        Instant timeNow = Instant.now();
        CompleteZNOResponse completeZNOResponse = new CompleteZNOResponse(203, "Успешно", "Запись успешно обновлена", timeNow);

        /* Настройка свойств продюсера */
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        MessageSender messageSender = new MessageSender(kafkaProps);

        /* Отправка сообщения в Kafka */
        var send = messageSender.sendMessage(topicName, 1L, completeZNOResponse);

        /* Результат отправки сообщения в Kafka */
        log.info("Into topic \"{}\" send message = {}, result = {}", topicName, completeZNOResponse, send);

        await().pollDelay(Duration.ofSeconds(3)).timeout(Duration.ofMinutes(1)).until(() -> true);

        /* Значения полей, которые меняет сервис */
        String actual2 = jdbcService.callDbSelect("SELECT id, processed, response_sm, response_sm_message, response_sm_date FROM surms.sm_queue_action");

        /* Преобразование даты в формат, который используется в PostgreSQL */
        String responseSmTimestamp = instant2Timestamp(timeNow);

        /* Ожидаемые значения полей в БД */
        String expected2 = String.format("203, t, Успешно, Запись успешно обновлена, %s\n", responseSmTimestamp);

        /* Проверка измененных полей в таблице surms.sm_queue_agreement с ожидаемым результатом */
        checkChangesInDatabase(expected2, actual2, topicName);
    }

    @Test
    @Order(value = 9)
    @AllureId("6591270")
    @DisplayName("ATM 09. CompleteZNOTaskProvider")
    @Tags({@Tag("kafka"), @Tag("deny-zno"), @Tag("ATM.09")})
    @Description(
            "Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.sm_queue_action, формирует и отправляет сообщение в топик topics.deny-zno-out.\n"
                    + "Этап II: проверяет, что после получения сообщения из топика topics.deny-zno-in сервис surms-apply-taskmanager вносит изменения в определенные поля родительской записи таблицы surms.sm_queue_action"
    )
    void testDenyZNOTaskProvider() throws Exception {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.deny-zno-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.deny-zno-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.deny-zno-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.deny-zno-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис */
        String sqlInsert1 = readFileFromResources("applytaskmanager/postgres/sql/initialization/09.service-manager-zno-deny-tasks.sql");

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, String>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщения в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Преобразование полученных ключей и сообщений в JsonNode */
        JsonNode receivedJsonNode = convertListMapEntryToJsonNode(allRecords);

        log.info("\nreceivedJsonNode = {}", receivedJsonNode);

        /* Ожидаемые сообщения из топики */
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(new File("src/test/resources/applytaskmanager/kafka/messages/expected/service-manager-zno-deny-tasks.json"));

        /* Проверка сообщений из топика service-manager-zno-deny-tasks с ожидаемым результатом */
        log.info("--- Проверка сообщений из топика service-manager-sd-create-tasks с ожидаемым результатом ---");
        checkGetMessageFromKafka(expectedJsonNode, receivedJsonNode, topicName);

        step("Этап II: Обработка сообщения из topics.deny-zno-in", () -> {});
        log.info("Этап II: Обработка сообщения из topics.deny-zno-in:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.deny-zno-in");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.deny-zno-in");
        System.out.println("groupId = " + groupId + "\n");

        /* Создание объекта для отправки в Kafka */
        Instant timeNow = Instant.now();
        DenyZNOResponse denyZNOResponse = new DenyZNOResponse(203, "Успешно", "Запись успешно обновлена", timeNow);

        /* Настройка свойств продюсера */
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        MessageSender messageSender = new MessageSender(kafkaProps);

        /* Отправка сообщения в Kafka */
        var send = messageSender.sendMessage(topicName, 1L, denyZNOResponse);

        /* Результат отправки сообщения в Kafka */
        log.info("Into topic \"{}\" send message = {}, result = {}", topicName, denyZNOResponse, send);

        await().pollDelay(Duration.ofSeconds(3)).timeout(Duration.ofMinutes(1)).until(() -> true);

        /* Значения полей, которые меняет сервис */
        String actual2 = jdbcService.callDbSelect("SELECT id, processed, response_sm, response_sm_message, response_sm_date FROM surms.sm_queue_action");

        /* Преобразование даты в формат, который используется в PostgreSQL */
        String responseSmTimestamp = instant2Timestamp(timeNow);

        /* Ожидаемые значения полей в БД */
        String expected2 = String.format("203, t, Успешно, Запись успешно обновлена, %s\n", responseSmTimestamp);

        /* Проверка измененных полей в таблице surms.sm_queue_agreement с ожидаемым результатом */
        checkChangesInDatabase(expected2, actual2, topicName);
    }

    @Test
    @Order(value = 10)
    @AllureId("6591271")
    @DisplayName("ATM 10. CompleteEsqSmTaskProvider")
    @Tags({@Tag("kafka"), @Tag("esq-sm"), @Tag("ATM.10")})
    @Description(
            "Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.employee_service_queue связанной с таблицами employee_queue, source_label, формирует и отправляет сообщение в топик topics.esq-sm--out.\n"
                    + "Этап II: проверяет, что после получения сообщения из топика topics.esq-sm--in сервис surms-apply-taskmanager вносит изменения в определенные поля родительской записи таблицы surms.employee_service_queue"
    )
    void testCompleteEsqSmTaskProvider() throws Exception {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.esq-sm-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.esq-sm-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.esq-sm-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.esq-sm-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = "INSERT INTO surms.employee_queue (employee_id,processed,source_id,is_locked,check_after,result_status_id) VALUES (455715,true,28,false,NULL,1)";
        String sqlInsert2 = "INSERT INTO surms.employee_service_queue (employee_queue_id,base_service_id,check_after,processed,result_status_id,result_comment,action_id,\"data\",processed_date,target) " +
                "VALUES ((select min(id) from surms.employee_queue where result_status_id = 1),142,'2025-03-24 08:54:50.125',false,1,'result_comment',1,'{\"serviceId\":35130}','2025-03-24 08:53:50.166','SM')";

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, Long> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, Long>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));
            log.info("jdbcService.callDbInsert(sqlInsert2) = \"{}\"", jdbcService.callDbInsert(sqlInsert2));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Ожидаемые сообщения из топики */
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree("{\"5906757\":236218}");

        /* Проверка сообщений из топика employee-service-queue-sm-tasks с ожидаемым результатом */
        log.info("--- Проверка сообщений из топика employee-service-queue-sm-tasks с ожидаемым результатом ---");
        checkGetMessageFromKafka(expectedJsonNode, allRecords, topicName);

        step("Этап II: Обработка сообщения из topics.esq-sm-in", () -> {});
        log.info("Этап II: Обработка сообщения из topics.esq-sm-in:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.esq-sm-in");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.esq-sm-in");
        System.out.println("groupId = " + groupId + "\n");

        /* Создание объекта для отправки в Kafka */
        LocalDateTime localTimeNow = LocalDateTime.now();
        JsonNode dataJsonNode = objectMapper.readTree(new File("src/test/resources/applytaskmanager/kafka/messages/externalized/data/employee-service-queue-sm-task-acks.json"));
        EsqDataResult esqDataResult = new EsqDataResult(236218, localTimeNow, "Доступ в HP SM по 'CI00903806' в процессе обработки по ID: SD0259408213", false, "null", dataJsonNode.toString());

        /* Настройка свойств продюсера */
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        MessageSender messageSender = new MessageSender(kafkaProps);

        /* Отправка сообщения в Kafka */
        var send = messageSender.sendMessage(topicName, 1L, esqDataResult);

        /* Результат отправки сообщения в Kafka */
        log.info("Into topic \"{}\" send message = {}, result = {}", topicName, esqDataResult, send);

        await().pollDelay(Duration.ofSeconds(3)).timeout(Duration.ofMinutes(1)).until(() -> true);

        /* Значения полей, которые меняет сервис */
        String actual2 = jdbcService.callDbSelect("SELECT id, check_after, processed, result_status_id, result_comment, data FROM surms.employee_service_queue");

        /* Преобразование даты в формат, который используется в PostgreSQL */
        String responseLocalDateTime = localDateTime2Timestamp(localTimeNow);

        /* Ожидаемые значения полей в БД */
        String expected2 = String.format("236218, %s, f, 15531, Доступ в HP SM по 'CI00903806' в процессе обработки по ID: SD0259408213, %s\n", responseLocalDateTime, dataJsonNode);

        /* Проверка измененных полей в таблице surms.sm_queue_agreement с ожидаемым результатом */
        checkChangesInDatabase(expected2, actual2, topicName);
    }

    @Test
    @Order(value = 11)
    @AllureId("6591272")
    @DisplayName("ATM 11. EsqAdTaskProvider")
    @Tags({@Tag("kafka"), @Tag("esq-ad"), @Tag("ATM.11")})
    @Description(
            "Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.employee_service_queue связанной с таблицами queue_action, queue_status, формирует и отправляет сообщение в топик topics.esq-ad-out.\n"
                    + "Этап II: проверяет, что после получения сообщения из топика topics.esq-ad-in сервис surms-apply-taskmanager вносит изменения в определенные поля родительской записи таблицы surms.employee_service_queue"
    )
    void testEsqAdTaskProvider() throws Exception {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.esq-ad-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.esq-ad-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.esq-ad-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.esq-ad-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        String sqlInsert1 = "INSERT INTO surms.employee_queue (employee_id,processed,source_id,is_locked,check_after,result_status_id) VALUES (455715,true,28,false,NULL,1)";
        String sqlInsert2 = "INSERT INTO surms.employee_service_queue (employee_queue_id,base_service_id,check_after,processed,result_status_id,result_comment,action_id,\"data\",processed_date,target) " +
                "VALUES ((select min(id) from surms.employee_queue where result_status_id = 1),142,'2025-03-24 08:54:50.125',false,1,'result_comment',1,'{\"serviceId\":35130}','2025-03-24 08:53:50.166','AD')";

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, String>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));
            log.info("jdbcService.callDbInsert(sqlInsert2) = \"{}\"", jdbcService.callDbInsert(sqlInsert2));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топики */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        /* Для информации: Вывод в лог полученные сообщения */
        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Преобразование полученных ключей и сообщений в JsonNode */
        JsonNode receivedJsonNode = convertListMapEntryToJsonNode(allRecords);

        log.info("\nreceivedJsonNode = {}", receivedJsonNode);

        /* Ожидаемые сообщения из топики */
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode expectedJsonNode = objectMapper.readTree(new File("src/test/resources/applytaskmanager/kafka/messages/expected/employee-service-queue-ad-tasks.json"));

        /* Проверка сообщений из топика employee-service-queue-ad-tasks с ожидаемым результатом */
        log.info("--- Проверка сообщений из топика employee-service-queue-ad-tasks с ожидаемым результатом ---");
        checkGetMessageFromKafka(expectedJsonNode, receivedJsonNode, topicName);

        step("Этап II: Обработка сообщения из topics.esq-ad-in", () -> {});
        log.info("Этап II: Обработка сообщения из topics.esq-ad-in:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.esq-ad-in");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.esq-ad-in");
        System.out.println("groupId = " + groupId + "\n");

        /* Создание объекта для отправки в Kafka */
        LocalDateTime localTimeNow = LocalDateTime.now();
        JsonNode dataJsonNode = objectMapper.readTree(new File("src/test/resources/applytaskmanager/kafka/messages/externalized/data/employee-service-queue-ad-task-acks.json"));
        EsqAdDataResult esqAdDataResult = new EsqAdDataResult(236218, localTimeNow, "Доступ по группам: [ALL-GA-IDFW-Access-Sigma-AC-PROM-CI04249079] предоставлен в домене Sigma", true, "DONE", dataJsonNode.toString());

        /* Настройка свойств продюсера */
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        MessageSender messageSender = new MessageSender(kafkaProps);

        /* Отправка сообщения в Kafka */
        var send = messageSender.sendMessage(topicName, 1L, esqAdDataResult);

        /* Результат отправки сообщения в Kafka */
        log.info("Into topic \"{}\" send message = {}, result = {}", topicName, esqAdDataResult, send);

        await().pollDelay(Duration.ofSeconds(3)).timeout(Duration.ofMinutes(1)).until(() -> true);

        /* Значения полей, которые меняет сервис */
        String actual2 = jdbcService.callDbSelect("SELECT id, check_after, result_status_id, result_comment, processed, action_id, data FROM surms.employee_service_queue");

        /* Преобразование даты в формат, который используется в PostgreSQL */
        String responseLocalDateTime = localDateTime2Timestamp(localTimeNow);

        /* Ожидаемые значения полей в БД */
        String expected2 = String.format("236218, %s, 1, Доступ по группам: [ALL-GA-IDFW-Access-Sigma-AC-PROM-CI04249079] предоставлен в домене Sigma, t, 1, %s\n", responseLocalDateTime, dataJsonNode);

        /* Проверка измененных полей в таблице surms.sm_queue_agreement с ожидаемым результатом */
        checkChangesInDatabase(expected2, actual2, topicName);
    }

    @Test
    @Order(value = 12)
    @AllureId("6591273")
    @DisplayName("ATM 12. EsqIdmTaskProvider")
    @Tags({@Tag("kafka"), @Tag("esq-idm"), @Tag("ATM.12")})
    @Description(
            "Этап I: проверяет, что сервис surms-apply-taskmanager в случае появления записи в таблице surms.employee_service_queue связанной с таблицами employee_queue, employee, base_service, queue_action, формирует и отправляет сообщение в топик topics.esq-idm-out.\n"
                    + "Этап II: проверяет, что после получения сообщения из топика topics.esq-idm-in сервис surms-apply-taskmanager вносит изменения в определенные поля родительской записи таблицы surms.employee_service_queue"
    )
    void testEsqIdmTaskProvider() throws Exception {

        /* Параметры для ЦОИФТ */
        setAllureLabelsATM();

        step("Этап I: Отправка сообщения в topics.esq-idm-out", () -> {});
        log.info("Этап I: Отправка сообщения в topics.esq-idm-out:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.esq-idm-out");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.esq-idm-out");
        System.out.println("groupId = " + groupId + "\n");

        /* Запросы добавления записей, на которые реагирует сервис surms-apply-taskmanager */
        ObjectMapper objectMapper = new ObjectMapper();
        String apply_summary = objectMapper.readTree(new File("src/test/resources/applytaskmanager/postgres/sql/initialization/12.esq-idm-out-applysummary.json")).toString();
        String sqlInsert1 = String.format("INSERT INTO surms.employee_queue (employee_id,processed,source_id,is_locked,check_after,result_status_id,apply_summary) VALUES (455715,true,28,false,NULL,1,'%s')", apply_summary);
        String sqlInsert2 = "INSERT INTO surms.employee_service_queue (employee_queue_id,base_service_id,check_after,processed,result_status_id,result_comment,action_id,\"data\",processed_date,target) " +
                "VALUES ((select min(id) from surms.employee_queue where result_status_id = 1),142,'2025-03-24 08:54:50.125',false,1,'result_comment',1,'{\"serviceId\":35130}','2025-03-24 08:53:50.166','IDM')";

        /* Настройка свойств консьюмера */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        /* Объявление консьюмера */
        KafkaConsumer<Long, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList(topicName));

        List<Map.Entry<Long, String>> allRecords;

        try {

            /* Считаем все лишние сообщения из топики */
            consumeAllRecords(kafkaConsumer);

            /* Подготовка БД, для инициализации сообщений в Kafka */
            /* Выполнение запросов, после которых сервис surms-apply-taskmanager сформирует сообщение в Kafka */
            log.info("jdbcService.callDbInsert(sqlInsert1) = \"{}\"", jdbcService.callDbInsert(sqlInsert1));
            log.info("jdbcService.callDbInsert(sqlInsert2) = \"{}\"", jdbcService.callDbInsert(sqlInsert2));

            await().pollDelay(Duration.ofSeconds(10)).timeout(Duration.ofMinutes(1)).until(() -> true);

            /* Получение сообщений из топика: employee-service-queue-idm-tasks */
            allRecords = consumeAllRecords(kafkaConsumer);

        } finally {
            kafkaConsumer.close();
        }

        loggingReceivedMessagesFromListMapEntry(allRecords);

        /* Преобразование полученных ключей и сообщений в JsonNode */
        JsonNode receivedJsonNode = convertListMapEntryToJsonNode(allRecords);

        log.info("\nreceivedJsonNode = {}", receivedJsonNode);

        /* Ожидаемые сообщения из топики */
        JsonNode expectedJsonNode = objectMapper.readTree(new File("src/test/resources/applytaskmanager/kafka/messages/expected/employee-service-queue-idm-tasks.json"));

        /* Проверка сообщений из топика employee-service-queue-idm-tasks с ожидаемым результатом */
        log.info("--- Проверка сообщений из топика employee-service-queue-idm-tasks с ожидаемым результатом ---");
        checkGetMessageFromKafka(expectedJsonNode, receivedJsonNode, topicName);

        step("Этап II: Обработка сообщения из topics.esq-idm-in", () -> {});
        log.info("Этап II: Обработка сообщения из topics.esq-idm-in:\n");

        topicName = param.getProperty("spring.kafka.properties.topic-name-mapping.esq-idm-in");
        System.out.println("topicName = " + topicName);
        groupId = param.getProperty("spring.kafka.properties.group-id-mapping.esq-idm-in");
        System.out.println("groupId = " + groupId + "\n");

        /* Создание объекта для отправки в Kafka */
        GeneralEsqDataResult generalEsqDataResult = new GeneralEsqDataResult(236218, true, "DONE", "Запись поставлена в обработку");

        /* Настройка свойств продюсера */
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSerializer.class.getName());

        MessageSender messageSender = new MessageSender(kafkaProps);

        /* Отправка сообщения в Kafka */
        var send = messageSender.sendMessage(topicName, 1L, generalEsqDataResult);

        /* Результат отправки сообщения в Kafka */
        log.info("Into topic \"{}\" send message = {}, result = {}", topicName, generalEsqDataResult, send);

        await().pollDelay(Duration.ofSeconds(3)).timeout(Duration.ofMinutes(1)).until(() -> true);

        /* Значения полей, которые меняет сервис */
        String actual2 = jdbcService.callDbSelect("SELECT id, result_status_id, result_comment, processed, action_id FROM surms.employee_service_queue");

        /* Ожидаемые значения полей в БД */
        String expected2 = "236218, 1, Запись поставлена в обработку, t, 1\n";

        /* Проверка измененных полей в таблице surms.sm_queue_agreement с ожидаемым результатом */
        checkChangesInDatabase(expected2, actual2, topicName);
    }
}

