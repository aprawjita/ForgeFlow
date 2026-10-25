package forgeflow_api.workflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowRequest {
    private Long connectedRepositoryId;
    private String name;
    private String configuration;
    private boolean enabled;
    private List<WorkflowJobDto> jobs;
}