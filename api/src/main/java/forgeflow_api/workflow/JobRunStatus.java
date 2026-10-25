package forgeflow_api.workflow;

public enum JobRunStatus {
    PENDING,
    QUEUED,
    RUNNING,
    SUCCESS,
    FAILURE,
    SKIPPED,
    CANCELLED,
    BLOCKED
}