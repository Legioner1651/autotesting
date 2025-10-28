package ru.ruslan.autotesting;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;
import ru.ruslan.autotesting.kafka.KafkaJsonDeserializer;
import ru.ruslan.autotesting.kafka.consumer.KafkaConsumerService;
import ru.ruslan.autotesting.kafka.producer.MessageSender;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZoneId;
import java.util.*;

@Testcontainers
public class ApplyTaskManagerBase extends AbstractGeneral {

    private static final Logger log = LoggerFactory.getLogger(ApplyTaskManagerBase.class);
    @Autowired
    private Param param;

    protected static final String ALIAS_CONTAINER_KAFKA = "kafkaAlias";
    protected static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:8.1.0");
    protected static final DockerImageName KAFKA_UI_IMAGE = DockerImageName.parse("provectuslabs/kafka-ui:v0.7.2");
//    protected static final String ALIAS_CONTAINER_KAFKA = "kafkaAlias";

    protected static final Network network = Network.newNetwork();
    protected static final ZoneId localTimeZone = ZoneId.systemDefault();
    protected static final boolean usageUI = true;

    protected static String bootstrapServersKafka = "localhost:9092";
    protected Properties kafkaProps;
    protected MessageSender messageSender;

    protected static ConfluentKafkaContainer containerKafka = new ConfluentKafkaContainer(KAFKA_IMAGE)
            .withNetwork(network)
            .withNetworkAliases(ALIAS_CONTAINER_KAFKA)
            .withEnv("TZ", localTimeZone.toString())
            .withStartupTimeout(Duration.ofMinutes(5));

    protected static GenericContainer containerKafkaUI = new GenericContainer(KAFKA_UI_IMAGE)
            .withNetworkMode("host")
            .dependsOn(containerKafka)
            .withEnv("TZ", localTimeZone.toString())
            .withEnv("KAFKA_CLUSTERS_0_NAME", "LOCAL_0")
            .withEnv("KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS", bootstrapServersKafka)
            .withEnv("KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL", "PLAINTEXT")
            .withEnv("KAFKA_CLUSTERS_0_CLUSTERS_SECURITY_PROTOCOL", "PLAINTEXT")
            .withEnv("AUTH_TYPE", "DISABLED");

    @BeforeAll
    public void beforeAll() {
        System.out.println("===== ===== ===== Код метода beforeAll() Base начал исполняться ===== ===== =====");

        log.info("Start of containerKafka");
        containerKafka.start();
        log.info("The containerKafka has already started");

        bootstrapServersKafka = containerKafka.getBootstrapServers();

        log.info("localTimeZone = \"{}\"", localTimeZone);
        log.info("bootstrapServersKafka = \"{}\"", bootstrapServersKafka);


        if (usageUI == true) {
            log.info("Start of containerKafkaUI");
            containerKafkaUI.withEnv("KAFKA_CLUSTERS_0_BOOTSTRAP_SERVERS", bootstrapServersKafka);
            containerKafkaUI.start();

            String hostAddress = containerKafkaUI.getHost();
            log.info("Доступ к Kafka-UI по адресу: {}:8080", hostAddress);
            log.info("The containerKafkaUI has already started");
        }

        kafkaProps = new Properties();
        kafkaProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);

        /*  Создание топиков в Kafka */
        try (AdminClient adminClient = AdminClient.create(kafkaProps)) {
            List<String> topicNames = new ArrayList<>();
            topicNames.add("topicName_1");
            topicNames.add("topicName_2");
            topicNames.add("topicName_3");

            /* Преобразование в объект Collection<NewTopic> */
            Collection<NewTopic> topics = new ArrayList<>(topicNames.size());
            for (String name : topicNames) { topics.add(new NewTopic(name, 1, (short) 1)); }

            CreateTopicsResult result = adminClient.createTopics(topics);
            log.info("Топики успешно созданы");
            log.info("topics.size() = {}", topics.size());
        }

        System.out.println("===== ===== ===== Код метода beforeAll() Base завершился ===== ===== =====");
    }

    @BeforeEach
    public void beforeEach() {
        System.out.println("===== ===== ===== Код метода beforeEach() Base начал исполняться ===== ===== =====");

//        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersKafka);
//        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
////        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonDeserializer.class.getName());
//        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        MessageSender messageSender = new MessageSender(kafkaProps);
//
//        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "topic_groupId");
//        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
//        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
//        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        KafkaConsumerService kafkaConsumerService = new KafkaConsumerService(kafkaProps);

        System.out.println("===== ===== ===== Код метода beforeEach() Base завершился ===== ===== =====");
    }

    @AfterAll
    public void afterAll() throws IOException {
        String fileName = String.format("LogKafka.log");
        writeText2File(containerKafka.getLogs(), Paths.get("target/" + fileName));

        if (usageUI == true) {
            log.info("Stop of containerKafkaUI");

            fileName = String.format("LogKafka_UI.log");
            writeText2File(containerKafkaUI.getLogs(), Paths.get("target/" + fileName));

            containerKafkaUI.stop();
            log.info("The containerKafkaUI has already stopped");
        }

        log.info("Stop of containerKafka");
        containerKafka.stop();
        log.info("The containerKafka has already stopped");
    }


    public void printSomeConfigKey2() {
        String value = param.getProperty("some.config.key2");
        System.out.println("some.config.key2: " + value);
    }

    public void printEnvLC_MONETARY() {
        String env = param.getEnv("LC_MONETARY");
        System.out.println("LC_MONETARY: " + env);
    }

    public static JsonNode convertListMapEntryToJsonNode(List<Map.Entry<String, JsonNode>> entries) {
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode result = mapper.createObjectNode();

        for (Map.Entry<String, JsonNode> entry : entries) {
            if (entry.getKey() != null && entry.getValue() != null) {
                result.set(entry.getKey(), entry.getValue());
            }
        }

        return result;
    }
}