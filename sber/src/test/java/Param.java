package ru.sberbank.sberuser.surms.test.integration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class Param {

    private final Environment environment;

    @Autowired
    public Param(Environment environment) {
        this.environment = environment;
    }

    public String getProperty(String propertyName) {
        return environment.getProperty(propertyName);
    }

    public String getEnv(String variableName) {
        return System.getenv(variableName);
    }

    /**
     * Получает список названий топиков из YAML-конфигурации.
     */
    public Collection<NewTopic> getTopicNamesFromYml() {
        List<String> topicNames = new ArrayList<>();

        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.eq-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.eq-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-sudir-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-sudir-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.apply-notification-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.apply-notification-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-esrt-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-esrt-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.post-apply-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.post-apply-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.create-zno-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.create-zno-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.create-sd-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.create-sd-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.complete-zno-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.complete-zno-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.deny-zno-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.deny-zno-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-sm-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-sm-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-ad-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-ad-in"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-idm-out"));
        topicNames.add(getProperty("spring.kafka.properties.topic-name-mapping.esq-idm-in"));

        // Преобразуем каждый строковый топик в объект NewTopic
        Collection<NewTopic> topics = new ArrayList<>(topicNames.size());
        for (String name : topicNames) {
            topics.add(new NewTopic(name, 1, (short) 1));
        }

        return topics;
    }
}
