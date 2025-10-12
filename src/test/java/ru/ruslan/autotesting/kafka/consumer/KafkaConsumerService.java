package ru.ruslan.autotesting.kafka.consumer;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import io.qameta.allure.model.StatusDetails;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Collections;
import java.util.Properties;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class KafkaConsumerService {

    private final Properties consumerProps;

    public KafkaConsumerService(Properties props) {
        this.consumerProps = props;
    }

    @Step("Получение сообщения из топика {topic}")
    public <K, V> CompletableFuture<ConsumerRecord<K, V>> consumeMessage(String topic) {
        CompletableFuture<ConsumerRecord<K, V>> future = new CompletableFuture<>();

        try (KafkaConsumer<K, V> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(topic));

            while (!future.isDone()) {
                ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100)); // Опрашиваем каждые 100 мс

                for (ConsumerRecord<K, V> record : records) {
                    future.complete(record); // Завершаем выполнение с результатом

                    Allure.getLifecycle().updateStep(stepResult ->
                            stepResult.setStatusDetails(new StatusDetails().setMessage(record.toString()))
                    );
                }
            }
        }

        return future;
    }
}

