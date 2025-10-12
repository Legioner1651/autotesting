package ru.ruslan.autotesting.kafka.producer;
package ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.producer;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import io.qameta.allure.model.StatusDetails;
import org.apache.kafka.clients.producer.*;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MessageSender {

    private final Properties producerProps;

    /**
     * Конструктор принимает конфигурационные свойства производителя Kafka.
     */
    public MessageSender(Properties producerProps) {
        this.producerProps = producerProps;
    }

    @Step("Отправка сообщения в топик {topic}")
    public <K, V> CompletableFuture<SendResult<K, V>> sendMessage(
            String topic,
            K key,
            V value
    ) throws ExecutionException, InterruptedException {
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
            Future<RecordMetadata> future = producer.send(record);

            // Ждем завершения отправки записи
            awaitSendCompletion(future);

            RecordMetadata metadata = future.get();

            // Формируем результат самостоятельно
            SendResult<K, V> result = new SendResult<>(
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp(), // Здесь доступна временная метка
                    key,
                    value
            );

            Allure.getLifecycle().updateStep(stepResult ->
                    stepResult.setStatusDetails(new StatusDetails().setMessage(metadata.toString())));
            return CompletableFuture.completedFuture(result);
        }
    }

    /**
     * Метод ожидает завершения отправки сообщения.
     *
     * @param future Будущее выполнение отправки записи
     */
    private void awaitSendCompletion(Future<?> future) {
        Awaitility.await()
                .timeout(Duration.ofMinutes(11))
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
//                .until(() -> future.isDone() && !future.isCompletedExceptionally());
                .until(future::isDone);
    }

    /**
     * Класс для хранения результатов отправки сообщения.
     */
    public static class SendResult<K, V> {
        private final String topic;
        private final int partition;
        private final long offset;
        private final long timestamp;
        private final K key;
        private final V value;

        public SendResult(String topic, int partition, long offset, long timestamp, K key, V value) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
        }

        // Методы геттеры...

        // Геттеры для всех полей:
        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }
}
