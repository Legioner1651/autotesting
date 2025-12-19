package taskmanager.kafka.domain;
package ru.sberbank.sberuser.surms.test.integration.apply.taskmanager.kafka.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateSDResponse {
    private long rowId;
    private String smResponse;
    private String commentSmResponse;
    private String objectId;
    private Instant createdAt;
}
