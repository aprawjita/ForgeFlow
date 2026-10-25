package forgeflow_api.orchestrator;

/**
 * Thrown when an Orchestrator fails to acquire the WorkflowRun lock.
 * This guarantees the Kafka listener does not acknowledge the event,
 * forcing redelivery so the orchestration is evaluated sequentially.
 */
public class ConcurrentOrchestrationException extends RuntimeException {
    public ConcurrentOrchestrationException(String message) {
        super(message);
    }
}