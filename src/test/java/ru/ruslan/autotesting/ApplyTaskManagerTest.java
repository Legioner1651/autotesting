package ru.ruslan.autotesting;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.junit.jupiter.api.Test;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ApplyTaskManagerTest extends ApplyTaskManagerBase {

    @Test
    public void testMessageFlowAndDBQuery() throws SQLException, ExecutionException, InterruptedException {
        String testYML = paramInstance.getProperty("topics.topic1");
        System.out.println("testYML = \"" + testYML + "\"");

        // Настройка producer для отправки сообщений в Kafka
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-id");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Отправляем сообщение в топик 'topic1'
        ProducerRecord<String, String> record = new ProducerRecord<>(
                paramInstance.getProperty("topics.topic1"), // Извлекаем имя топика из YML-файла
                "Hello world!!! OK 1"
        );
        producer.send(record).get();
        producer.close();

        // Чтение сообщений из Kafka
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singleton(paramInstance.getProperty("topics.topic1")));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> r : records) {
            log.info("Received message from Kafka: {}", r.value()); // выводим полученное сообщение
        }
        consumer.close();

        // Выполняем простой SQL-запрос к PostgreSQL
        Connection connection = postgresqlContainer.createConnection("");
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM pg_database;");
        while(resultSet.next()){
            log.info("Result of DB Query: {}", resultSet.getString(1)); // выводим результат запроса
        }

        // Читаем значение параметра из application.yml
        String configValue = readConfigValueFromYML();
        log.info("Config value from YAML file is: {}", configValue); // выводим значение параметра
    }
}
