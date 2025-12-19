package taskmanager.kafka.consumer;

import io.qameta.allure.Allure;
import io.qameta.allure.Step;
import io.qameta.allure.model.StatusDetails;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.util.Map.Entry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class KafkaConsumerService {

    /**
     * Получаем ВСЕ доступные записи через уже созданный и настроенный потребитель.
     *
     * @param consumer Подписанный и готовый экземпляр KafkaConsumer.
     * @return Список пар ключ-значение полученных записей.
     */
    @Step("Получение всех записей из топика")
    public static <K, V> List<Entry<K, V>> consumeAllRecords(KafkaConsumer<K, V> consumer) {
        log.info("Попытка получить сообщения из топика: ");

        int countMessages = 0;
        List<Map.Entry<K, V>> recordsList = new ArrayList<>();

        try {

            boolean hasMoreMessages = true;
            while (hasMoreMessages) {

                ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(30));
                countMessages += records.count();

                if (!records.isEmpty()) {

                    for (ConsumerRecord<K, V> record : records) {
                        K key = record.key();
                        V value = record.value();

                        recordsList.add(Map.entry(key, value));
                        log.debug("record : Key = {}, Value = {}", key, value);
                    }
                }

                // Если получена пустая партия записей — считаем, что больше сообщений нет
                hasMoreMessages = !records.isEmpty();
            }

            return recordsList;

        } catch (Exception ex) {
            throw new RuntimeException("Ошибка при получении записей из Kafka", ex);
        } finally {
            log.info("Закончено получение сообщений из топика.");
            log.info("Количество скачанных записей = {}", countMessages);
        }
    }
}
