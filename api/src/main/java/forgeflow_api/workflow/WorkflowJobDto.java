package forgeflow_api.workflow;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WorkflowJobDto {
    private String name;
    private Set<String> dependsOnJobNames;
    private List<WorkflowStepDto> steps;
    private List<String> produces;
    private List<String> consumes;
}