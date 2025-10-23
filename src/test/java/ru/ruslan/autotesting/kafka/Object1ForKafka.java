package ru.ruslan.autotesting.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/* CreateZNOResponse */
/* CreateSDResponse */
/* CompleteZNOResponse without objectID */
/* DenyZNOResponse without objectID */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Object1ForKafka {
    private long rowID;
    private String smResponse;
    private String commentSmResponse;
    private String objectID;
    private Instant createdAt;
}
