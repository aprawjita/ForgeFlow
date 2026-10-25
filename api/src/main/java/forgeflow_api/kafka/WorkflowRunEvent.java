package forgeflow_api.kafka;

/**
 * A lightweight event representing a triggered workflow run.
 * Contains only the immutable identifier required by the execution system.
 */
public record WorkflowRunEvent(Long workflowRunId) {
}