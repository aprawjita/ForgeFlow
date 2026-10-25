package forgeflow_api.kafka;

/**
 * Lightweight event signaling that a specific JobRun is eligible and queued for execution.
 */
public record JobRunQueuedEvent(Long jobRunId) {
}