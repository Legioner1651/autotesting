package ru.ruslan.autotesting;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class Param {

    private final Environment env;

    @Autowired
    public Param(Environment environment) {
        this.env = environment;
    }

    // Метод для получения свойства из YML
    public String getProperty(String key) {
        return "topic-test-1";
//        return env.getProperty(key);      // ******************************
    }

    // Получение списка всех длинных имен топиков
    public List<String> getTopicNames() {
        List<String> topics = new ArrayList<>();
        topics.add("topic-test-1");
        topics.add("topic-test-2");
        topics.add("topic-test-3");
        topics.add("topic-test-4");
        return topics;
//        Map<String, Object> topicsMap = env.getProperty("topics", Map.class);     // ***************************
//        if (topicsMap != null && !topicsMap.isEmpty()) {
//            return topicsMap.values().stream()
//                    .map(Object::toString)
//                    .toList();
//        }
//        return List.of(); // пустой список, если топики отсутствуют
    }
}
