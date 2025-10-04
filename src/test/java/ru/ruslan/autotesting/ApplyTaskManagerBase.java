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

    protected static KafkaContainer kafkaContainer;
    protected PostgreSQLContainer<?> postgresqlContainer;
    protected DataSource dataSource;
    protected static Param paramInstance;

    @BeforeAll
    static void initParamInstance() {
        System.out.println("*** Start @BeforeAll initParamInstance ***");
        paramInstance = new Param(null); // временно используем фиктивный аргумент, поскольку среда тестирования не настроена на загрузку конфигурации автоматически
        System.out.println("*** Start @BeforeAll initParamInstance ***");
    }

    @BeforeAll
    static void startKafkaContainer() throws Exception {
        System.out.println("*** Start @BeforeAll ***");
//        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
//        loggerContext.getLogger("org.apache.zookeeper").setLevel(Level.ERROR); // Подавляем лишние логи Zookeeper

        kafkaContainer = new KafkaContainer("6.2.4");
//        kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka:3.7.1"));
//        DockerImageName kafkaImage = DockerImageName.parse("apache/kafka:3.7.1").asCompatibleSubstituteFor("confluentinc/cp-kafka");
//        kafkaContainer = new KafkaContainer(kafkaImage);
        kafkaContainer.start();

        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        try (var adminClient = org.apache.kafka.clients.admin.Admin.create(adminProps)) {
            List<String> topicsToCreate = paramInstance.getTopicNames();
            System.out.println("topicsToCreate = \"" + topicsToCreate + "\"");
            for (String topic : topicsToCreate) {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            }
        }

        System.out.println("*** End @BeforeAll ***");
    }

    @BeforeEach
    void startPostgresContainer() {
        System.out.println("*** Start @BeforeEach ***");
        postgresqlContainer = new PostgreSQLContainer<>(DockerImageName.parse("postgres:13-alpine"))
                .withDatabaseName("testdb")
                .withUsername("testuser")
                .withPassword("testpass");
        postgresqlContainer.start();
        System.out.println("*** End @BeforeEach ***");
    }

    @AfterEach
    void stopPostgresContainer() {
        System.out.println("*** Start @AfterEach ***");
        if (postgresqlContainer != null) {
            postgresqlContainer.stop();
        }
        System.out.println("*** End @AfterEach ***");
    }

    @AfterAll
    static void stopKafkaContainer() {
        System.out.println("*** Start @AfterAll ***");
        if (kafkaContainer != null) {
            kafkaContainer.stop();
        }
        System.out.println("*** End @AfterAll ***");
    }

    // Используем Param для чтения значения параметра
    public String readConfigValueFromYML() {
        return paramInstance.getProperty("some.config.key");
    }
}
