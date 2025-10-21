package ru.ruslan.autotesting.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class KafkaJsonDeserializer<T> implements Deserializer<T> {

    private Class<T> typeClass;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    // Конструктор для использования с конфигурацией через configure()
    public KafkaJsonDeserializer() {
        // ObjectMapper уже инициализирован
    }

    // Конструктор для явного указания класса
    public KafkaJsonDeserializer(Class<T> typeClass) {
        this();
        this.typeClass = typeClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Получаем тип из конфигурации, если не был установлен в конструкторе
        if (typeClass == null) {
            String typeClassConfig = isKey ?
                "key.deserializer.type" : "value.deserializer.type";
            String className = (String) configs.get(typeClassConfig);
            try {
                if (className == null) {
                    throw new SerializationException(
                        "Не указан тип для десериализации. Установите свойство " + typeClassConfig
                    );
                }
                this.typeClass = (Class<T>) Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new SerializationException(
                    "Класс " + className + " не найден", e
                );
            }
        }
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        if (typeClass == null) {
            throw new SerializationException(
                "Type class not configured for deserialization. " +
                "Please configure using constructor or configure() method"
            );
        }

        try {
            return objectMapper.readValue(bytes, typeClass);
        } catch (IOException e) {
            throw new SerializationException("Ошибка десериализации JSON-объекта", e);
        }
    }

    @Override
    public void close() {}
}
