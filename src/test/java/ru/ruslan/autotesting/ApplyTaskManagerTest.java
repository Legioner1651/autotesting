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

        // Читаем значение параметра из application.yml
        String configValue = readConfigValueFromYML();
        log.info("Config value from YAML file is: {}", configValue); // выводим значение параметра
    }
}
