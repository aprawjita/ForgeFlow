package forgeflow_api.worker;

import forgeflow_api.kafka.JobRunQueuedEvent;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class WorkerKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(WorkerKafkaConsumer.class);
    private final WorkerService workerService;

    @KafkaListener(topics = "${forgeflow.kafka.topic.job-run-queued}", groupId = "forgeflow-worker-group")
    public void consumeJobRunQueued(JobRunQueuedEvent event) {
        logger.info("Worker received JobRunQueuedEvent for JobRun ID: {}", event.jobRunId());
        try {
            workerService.executeJob(event.jobRunId());
        } catch (Exception e) {
            logger.error("Worker encountered an error processing JobRun ID: {}", event.jobRunId(), e);
        }
    }
}