package forgeflow_api.orchestrator;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.worker.DistributedLockService;
import forgeflow_api.workflow.*;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrchestratorService {

    private final DistributedLockService distributedLockService;
    private final JobRunDao jobRunDao;
    private final WorkflowRunDao workflowRunDao;
    private final WorkflowJobDao workflowJobDao;
    private final KafkaProducerService kafkaProducerService;
    private final MeterRegistry meterRegistry;

    public void processJobCompletion(Long jobRunId) {
        JobRun completedJobRun = jobRunDao.findById(jobRunId)
                .orElseThrow(() -> new IllegalArgumentException("JobRun not found in database: " + jobRunId));

        WorkflowRun workflowRun = completedJobRun.getWorkflowRun();
        Long workflowRunId = workflowRun.getId();
        String lockKey = "forgeflow:lock:workflowrun:" + workflowRunId;

        boolean locked = false;
        try {
            try {
                locked = distributedLockService.tryLock(lockKey);
            } catch (Exception e) {
                log.error("Redis connection failed while attempting to lock workflowRun: {}", workflowRunId, e);
                throw new RuntimeException("Redis unavailable. Failing closed.", e);
            }

            if (!locked) {
                log.info("WorkflowRun {} is currently being orchestrated by another node. Event will be retried.", workflowRunId);
                throw new ConcurrentOrchestrationException("Could not acquire lock for workflowRun: " + workflowRunId);
            }

            log.info("Acquired lock for WorkflowRun {}. Evaluating DAG for JobRun {}", workflowRunId, jobRunId);

            // 1. Fetch entire runtime state to avoid N+1 queries
            List<JobRun> allJobRuns = jobRunDao.findByWorkflowRun(workflowRun);
            Map<Long, JobRun> jobRunByWorkflowJobId = allJobRuns.stream()
                    .collect(Collectors.toMap(jr -> jr.getWorkflowJob().getId(), jr -> jr));

            // 2. Initialize BFS Queue with immediate downstreams
            Queue<WorkflowJob> queue = new LinkedList<>(workflowJobDao.findByDependsOnContaining(completedJobRun.getWorkflowJob()));

            while (!queue.isEmpty()) {
                WorkflowJob downstreamJob = queue.poll();
                JobRun downstreamRun = jobRunByWorkflowJobId.get(downstreamJob.getId());

                // Idempotency: Ignore jobs that are already processed or queued
                if (downstreamRun == null || downstreamRun.getStatus() != JobRunStatus.PENDING) {
                    continue;
                }

                boolean hasFailureOrSkipped = false;
                boolean allSuccess = true;

                // Evaluate all upstream dependencies for this downstream job
                for (WorkflowJob upstreamJob : downstreamJob.getDependsOn()) {
                    JobRun upstreamRun = jobRunByWorkflowJobId.get(upstreamJob.getId());
                    if (upstreamRun == null) {
                        allSuccess = false;
                        continue;
                    }
                    
                    JobRunStatus upStatus = upstreamRun.getStatus();
                    if (upStatus == JobRunStatus.FAILURE || upStatus == JobRunStatus.SKIPPED || upStatus == JobRunStatus.CANCELLED) {
                        hasFailureOrSkipped = true;
                    }
                    if (upStatus != JobRunStatus.SUCCESS) {
                        allSuccess = false;
                    }
                }

                // Apply state transitions
                if (hasFailureOrSkipped) {
                    downstreamRun.setStatus(JobRunStatus.SKIPPED);
                    jobRunDao.save(downstreamRun);
                    log.info("JobRun {} marked SKIPPED due to upstream failure/skip.", downstreamRun.getId());
                    
                    // Cascade Skip: Immediately enqueue its downstreams
                    queue.addAll(workflowJobDao.findByDependsOnContaining(downstreamJob));
                    
                } else if (allSuccess) {
                    downstreamRun.setStatus(JobRunStatus.QUEUED);
                    jobRunDao.save(downstreamRun);
                    log.info("JobRun {} marked QUEUED. All upstreams SUCCESS.", downstreamRun.getId());
                    
                    // Publish standard Kafka queued event passing the Long ID directly
                    kafkaProducerService.publishJobRunQueued(downstreamRun.getId());
                }
                // Otherwise: Remains PENDING (Waiting on other upstreams)
            }

            // 3. WorkflowRun Terminal State Resolution
            boolean allTerminal = true;
            boolean anyFailure = false;

            for (JobRun jr : jobRunByWorkflowJobId.values()) {
                JobRunStatus status = jr.getStatus();
                if (status == JobRunStatus.PENDING || status == JobRunStatus.QUEUED || status == JobRunStatus.RUNNING) {
                    allTerminal = false;
                    break;
                }
                if (status == JobRunStatus.FAILURE) {
                    anyFailure = true;
                }
            }

            // Only transition if the entire graph is terminal and it hasn't already been marked
            if (allTerminal && workflowRun.getStatus() != RunStatus.SUCCESS && workflowRun.getStatus() != RunStatus.FAILURE) {
                if (anyFailure) {
                    workflowRun.setStatus(RunStatus.FAILURE);
                    log.info("WorkflowRun {} completed with FAILURE.", workflowRunId);
                } else {
                    workflowRun.setStatus(RunStatus.SUCCESS);
                    log.info("WorkflowRun {} completed with SUCCESS.", workflowRunId);
                }
                workflowRunDao.save(workflowRun);

                // --- PHASE 17 PART 2B METRICS ---
                // Metric 1: Record durable terminal transition
                meterRegistry.counter("forgeflow_workflow_runs", "status", workflowRun.getStatus().name()).increment();
                
                // Metric 2: Record duration (strictly preserves existing creation->completion null-semantics)
                if (workflowRun.getStartedAt() != null && workflowRun.getCompletedAt() != null) {
                    meterRegistry.timer("forgeflow_workflow_duration", "status", workflowRun.getStatus().name())
                            .record(Duration.between(workflowRun.getStartedAt(), workflowRun.getCompletedAt()));
                }
            }

        } finally {
            if (locked) {
                distributedLockService.unlock(lockKey);
                log.info("Released lock for WorkflowRun {}", workflowRunId);
            }
        }
    }
}