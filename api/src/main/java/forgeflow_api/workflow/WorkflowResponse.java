package forgeflow_api.workflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WorkflowResponse {
    private Long id;
    private Long repositoryId;
    private String name;
    private String configuration;
    private boolean enabled;
    private List<WorkflowJobDto> jobs;
    private Instant createdAt;
    private Instant updatedAt;
}