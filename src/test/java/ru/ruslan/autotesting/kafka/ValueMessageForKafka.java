package ru.ruslan.autotesting.kafka;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

public record ValueMessageForKafka(
        Long id,
        String surname,
        String name,
        String patronymic,
        Instant instant,
        LocalDateTime localDateTime,
        ZonedDateTime zdt,
        NewTypeObject newTypeObject
) {}

record NewTypeObject(
        Long id,
        String name,
        String jsonObject
) {}