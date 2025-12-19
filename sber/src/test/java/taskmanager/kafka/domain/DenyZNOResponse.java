package taskmanager.kafka.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DenyZNOResponse {
    private long rowId;
    private String smResponse;
    private String commentSmResponse;
    private Instant createdAt;
}
