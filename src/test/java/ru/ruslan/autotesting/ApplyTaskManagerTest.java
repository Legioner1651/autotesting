package ru.ruslan.autotesting;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import ru.ruslan.autotesting.kafka.KafkaJsonDeserializer;
import ru.ruslan.autotesting.kafka.consumer.KafkaConsumerService;
import ru.ruslan.autotesting.kafka.producer.MessageSender;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ApplyTaskManagerTest extends ApplyTaskManagerBase {

    @Autowired
    private Param param;

//    protected Properties kafkaProps = new Properties();

    String value_45;

    @BeforeAll
    public void beforeAll2() {
        System.out.println("===== ----- *** Код метода beforeAll() Test начал исполняться *** ----- =====");
        value_45 = param.getProperty("some.config.key4");
        System.out.println("some.config.key4: " + value_45);
        System.out.println("===== ----- *** Код метода beforeAll() Test завершился *** ----- =====");
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
    void testPropertiesAndEnvironment2() throws ExecutionException, InterruptedException {
        System.out.println("===== Start @Test 2 =====");

        /* ********** Отправка сообщения в Кафку ********** */
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        log.info("Значение kafkaProps = {}", kafkaProps);

        MessageSender messageSender = new MessageSender(kafkaProps);

        log.info("Значение messageSender = {}", messageSender);

        var send = messageSender.sendMessage("topicName_1", 1L, "Test message N 1");
        log.info("Отправка сообщения в Кафку, результат = {}", send);

        Awaitility.await().pollDelay(Duration.ofMinutes(1)).timeout(Duration.ofMinutes(2));
        /* ********** Получение сообщений из Кафки ********** */
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "topic_groupId");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumerService kafkaConsumerService = new KafkaConsumerService(kafkaProps);

        List<Map.Entry<Object, String>> allRecords = kafkaConsumerService.ConsumerAllRecords("topicName_1");

        allRecords.forEach(entry -> {
            System.out.println("Ключ: " + entry.getKey() + ", Значение: " + entry.getValue());
        });

        System.out.println("===== End @Test 2 =====");
    }
}