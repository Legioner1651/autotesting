package taskmanager.kafka.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GeneralEsqDataResult {
    private long esqId;
    private Boolean processed;
    private String status;
    private String resultComment;
}
