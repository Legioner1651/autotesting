package ru.ruslan.autotesting;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

// Основной класс, который служит базой для последующих классов-тестов
// Этот класс чисто JUnit5, без зависимостей от Spring
public abstract class ApplyTaskManagerBase {

    protected static Param paramInstance;

    @BeforeAll
    static void initParamInstance() {
        System.out.println("*** Start @BeforeAll initParamInstance ***");
        System.out.println("*** Start @BeforeAll initParamInstance ***");
    }

    @BeforeAll
    static void startKafkaContainer() throws Exception {
        System.out.println("*** Start @BeforeAll ***");

        System.out.println("*** End @BeforeAll ***");
    }

    @BeforeEach
    void startPostgresContainer() {
        System.out.println("*** Start @BeforeEach ***");

        System.out.println("*** End @BeforeEach ***");
    }

    @AfterEach
    void stopPostgresContainer() {
        System.out.println("*** Start @AfterEach ***");

        System.out.println("*** End @AfterEach ***");
    }

    @AfterAll
    static void stopKafkaContainer() {
        System.out.println("*** Start @AfterAll ***");

        System.out.println("*** End @AfterAll ***");
    }

    // Используем Param для чтения значения параметра
    public String readConfigValueFromYML() {
        return paramInstance.getProperty("some.config.key");
    }
}
