package taskmanager.kafka.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EsqAdDataResult {
    private long esqId;
    private LocalDateTime checkAfter;
    private String resultComment;
    private Boolean processed;
    private String status;
    private String data;
}
