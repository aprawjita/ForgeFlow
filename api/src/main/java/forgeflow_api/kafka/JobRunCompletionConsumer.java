package forgeflow_api.kafka;

import forgeflow_api.workflow.WorkflowOrchestratorService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class JobRunCompletionConsumer {

    private final WorkflowOrchestratorService orchestratorService;

    @KafkaListener(topics = "${forgeflow.kafka.topic.job-run-completed}", groupId = "forgeflow-orchestrator-group")
    public void consumeJobRunCompleted(JobRunCompletedEvent event) {
        orchestratorService.evaluateDagProgression(event.workflowRunId());
    }
}