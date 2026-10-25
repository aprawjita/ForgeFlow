package forgeflow_api.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String workflowTriggeredTopic;
    private final String jobQueuedTopic;
    private final String jobCompletedTopic;

    public KafkaProducerService(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${forgeflow.kafka.topic.workflow-run-triggered}") String workflowTriggeredTopic,
            @Value("${forgeflow.kafka.topic.job-run-queued}") String jobQueuedTopic,
            @Value("${forgeflow.kafka.topic.job-run-completed}") String jobCompletedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.workflowTriggeredTopic = workflowTriggeredTopic;
        this.jobQueuedTopic = jobQueuedTopic;
        this.jobCompletedTopic = jobCompletedTopic;
    }

    public void publishWorkflowRunTriggered(Long workflowRunId) {
        WorkflowRunEvent event = new WorkflowRunEvent(workflowRunId);
        sendWithTimeout(workflowTriggeredTopic, String.valueOf(workflowRunId), event);
    }

    public void publishJobRunQueued(Long jobRunId) {
        JobRunQueuedEvent event = new JobRunQueuedEvent(jobRunId);
        sendWithTimeout(jobQueuedTopic, String.valueOf(jobRunId), event);
    }

    public void publishJobRunCompleted(JobRunCompletedEvent event) {
        sendWithTimeout(jobCompletedTopic, String.valueOf(event.jobRunId()), event);
    }

    private void sendWithTimeout(String topic, String key, Object payload) {
        try {
            kafkaTemplate.send(topic, key, payload).get(5, TimeUnit.SECONDS);
            logger.info("Published event to topic: {}", topic);
        } catch (Exception e) {
            logger.error("Failed to publish event to Kafka topic: {}", topic, e);
            throw new RuntimeException("Kafka publication failed", e);
        }
    }
}