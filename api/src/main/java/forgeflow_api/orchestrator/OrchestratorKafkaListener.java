package forgeflow_api.orchestrator;

import forgeflow_api.kafka.JobRunCompletedEvent; 
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrchestratorKafkaListener {

    private final OrchestratorService orchestratorService;

    @KafkaListener(topics = "job-run-completed", groupId = "forgeflow-orchestrator-group")
    public void onJobRunCompleted(JobRunCompletedEvent event) {
        log.info("Orchestrator received job-run-completed event for JobRun ID: {}", event.jobRunId());
        
        // Pass strictly the ID to enforce PostgreSQL source-of-truth lookups
        orchestratorService.processJobCompletion(event.jobRunId());
    }
}