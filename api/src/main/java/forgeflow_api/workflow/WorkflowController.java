package forgeflow_api.workflow;

import forgeflow_api.user.User;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class WorkflowController {

    private final WorkflowService workflowService;

    @PostMapping("/repositories/{repositoryId}/workflows")
    public ResponseEntity<WorkflowResponse> createWorkflow(
            @PathVariable Long repositoryId,
            @Valid @RequestBody WorkflowRequest request,
            @AuthenticationPrincipal User currentUser
    ) {
        return ResponseEntity.ok(workflowService.createWorkflow(repositoryId, request, currentUser));
    }

    @GetMapping("/repositories/{repositoryId}/workflows")
    public ResponseEntity<List<WorkflowResponse>> getWorkflowsForRepository(
            @PathVariable Long repositoryId,
            @AuthenticationPrincipal User currentUser
    ) {
        return ResponseEntity.ok(workflowService.getWorkflowsForRepository(repositoryId, currentUser));
    }

    @GetMapping("/workflows/{workflowId}")
    public ResponseEntity<WorkflowResponse> getWorkflowById(
            @PathVariable Long workflowId,
            @AuthenticationPrincipal User currentUser
    ) {
        return ResponseEntity.ok(workflowService.getWorkflowById(workflowId, currentUser));
    }

    @PutMapping("/workflows/{workflowId}")
    public ResponseEntity<WorkflowResponse> updateWorkflow(
            @PathVariable Long workflowId,
            @Valid @RequestBody WorkflowRequest request,
            @AuthenticationPrincipal User currentUser
    ) {
        return ResponseEntity.ok(workflowService.updateWorkflow(workflowId, request, currentUser));
    }

    @DeleteMapping("/workflows/{workflowId}")
    public ResponseEntity<Void> deleteWorkflow(
            @PathVariable Long workflowId,
            @AuthenticationPrincipal User currentUser
    ) {
        workflowService.deleteWorkflow(workflowId, currentUser);
        return ResponseEntity.noContent().build();
    }
}