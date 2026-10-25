package forgeflow_api.kafka;

import forgeflow_api.workflow.JobRunStatus;

public record JobRunCompletedEvent(
        Long jobRunId,
        Long workflowRunId,
        JobRunStatus status
) {}