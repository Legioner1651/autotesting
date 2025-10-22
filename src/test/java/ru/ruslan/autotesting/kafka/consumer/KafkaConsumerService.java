package ru.ruslan.autotesting.kafka.consumer;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import io.qameta.allure.model.StatusDetails;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class KafkaConsumerService {

    private final Properties consumerProps;

    public KafkaConsumerService(Properties props) {
        this.consumerProps = props;
    }

    @Step("Получение сообщения из топика {topic}")
    public <K, V> List<Map.Entry<K, V>> ConsumerAllRecords(String topic) {
//        CompletableFuture<ConsumerRecord<K, V>> future = new CompletableFuture<>();

        List<Map.Entry<K, V>> recordList = new ArrayList<>();

        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));

            boolean hasMoreMessages = true;

            while (hasMoreMessages) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100)); // Опрашиваем каждые 100 мс

                log.info("records.count() = {}", records.count());
                log.info("- 5 -");

                for (ConsumerRecord<K, V> record : records) {
                    K key = record.key();
                    V value = record.value();

                    recordList.add(Map.entry(key, value));
                    log.info("ConsumerAllRecords Key:Value = {} : {}", key, value);

                    Allure.getLifecycle().updateStep(stepResult ->
                            stepResult.setStatusDetails(new StatusDetails().setMessage(record.toString()))
                    );
                    log.info("- 4 -");
                }
                log.info("- 3 -");
                hasMoreMessages = !records.isEmpty();
            }
            log.info("- 2 -");
        }

        log.info("- 1 -");
//        return future;
        return recordList;
    }
}

