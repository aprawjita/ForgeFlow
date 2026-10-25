package forgeflow_api.kafka;

import forgeflow_api.workflow.WorkflowOrchestratorService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final WorkflowOrchestratorService orchestratorService;

    @KafkaListener(topics = "${forgeflow.kafka.topic.workflow-run-triggered}", groupId = "forgeflow-orchestrator-group")
    public void consumeWorkflowRunTriggered(WorkflowRunEvent event) {
        logger.info("Consumed WorkflowRunEvent for Run ID: {}", event.workflowRunId());
        try {
            orchestratorService.orchestrateWorkflowRun(event.workflowRunId());
        } catch (Exception e) {
            logger.error("Failed to orchestrate workflow run ID: {}", event.workflowRunId(), e);
        }
    }
}