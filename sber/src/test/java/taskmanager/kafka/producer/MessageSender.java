package taskmanager.kafka.producer;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import io.qameta.allure.model.StatusDetails;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class MessageSender {

    private final Properties producerProps;

    /**
     * Конструктор принимает конфигурационные свойства производителя Kafka.
     */
    public MessageSender(Properties producerProps) {
        this.producerProps = producerProps;
    }

    @Step("Отправка сообщения в топик {topic}")
    public <K, V> SendResult<K, V> sendMessage(
            String topic,
            K key,
            V value
    ) throws Exception {
        try (KafkaProducer<K, V> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);

            // Отправляем сообщение и ждём его завершения сразу же
            RecordMetadata metadata = producer.send(record).get(); // Блокируем поток до завершения

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

            return result;
        } catch (Exception e) {
            throw new RuntimeException("Ошибка отправки сообщения", e); // Или выбрасываем иное исключение
        }
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
