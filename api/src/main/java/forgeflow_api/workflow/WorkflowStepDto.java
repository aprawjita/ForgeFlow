package forgeflow_api.workflow;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class WorkflowStepDto {
    @NotBlank(message = "Step name is required")
    private String name;

    @NotBlank(message = "Step command is required")
    private String command;

    private int executionOrder;
}