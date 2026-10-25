package forgeflow_api.worker;

import forgeflow_api.kafka.JobRunCompletedEvent;
import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.workflow.*;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class WorkerService {

    private final JobRunDao jobRunDao;
    private final StepRunDao stepRunDao;
    private final DistributedLockService distributedLockService;
    private final StepExecutionEngine stepExecutionEngine;
    private final KafkaProducerService kafkaProducerService;
    private final MeterRegistry meterRegistry;

    public void executeJob(Long jobRunId) {
        String lockKey = "forgeflow:lock:jobrun:" + jobRunId;
        boolean locked = false;

        try {
            locked = distributedLockService.tryLock(lockKey);
            if (!locked) {
                log.info("JobRun {} is currently locked by another worker. Aborting safely.", jobRunId);
                return;
            }

            JobRun jobRun = jobRunDao.findById(jobRunId)
                    .orElseThrow(() -> new IllegalArgumentException("JobRun not found: " + jobRunId));

            // Idempotency guarantee: prevents duplicate execution and metric double-counting
            if (jobRun.getStatus() != JobRunStatus.QUEUED) return;

            List<WorkflowStep> steps = new ArrayList<>(jobRun.getWorkflowJob().getSteps());
            steps.sort(Comparator.comparing(WorkflowStep::getExecutionOrder));

            List<StepRun> stepRuns = new ArrayList<>();
            for (WorkflowStep step : steps) {
                stepRuns.add(stepRunDao.save(StepRun.builder()
                        .jobRun(jobRun).workflowStep(step).status(StepRunStatus.PENDING).build()));
            }

            jobRun.setStatus(JobRunStatus.RUNNING);
            jobRun.setStartedAt(Instant.now()); // Note: Suppressed by @CreationTimestamp updatable=false
            jobRunDao.save(jobRun);

            boolean allSuccess = true;
            AtomicLong logSequence = new AtomicLong(0);

            for (int i = 0; i < steps.size(); i++) {
                StepRun stepRun = stepRuns.get(i);
                stepRun.setStatus(StepRunStatus.RUNNING);
                stepRunDao.save(stepRun);

                int exitCode;
                try {
                    exitCode = stepExecutionEngine.executeStep(jobRun, stepRun, logSequence);
                } catch (Exception e) {
                    log.error("Execution engine threw exception on step: {}", stepRun.getWorkflowStep().getName(), e);
                    exitCode = -1;
                }

                if (exitCode == 0) {
                    stepRun.setStatus(StepRunStatus.SUCCESS);
                    stepRun.setCompletedAt(Instant.now());
                    stepRunDao.save(stepRun); // Reverted to not overwrite with mock null
                    recordStepMetrics(stepRun);
                } else {
                    stepRun.setStatus(StepRunStatus.FAILURE);
                    stepRun.setCompletedAt(Instant.now());
                    stepRunDao.save(stepRun); // Reverted to not overwrite with mock null
                    recordStepMetrics(stepRun);
                    allSuccess = false;
                    break;
                }
            }

            if (!allSuccess) {
                for (StepRun sr : stepRuns) {
                    if (sr.getStatus() == StepRunStatus.PENDING) {
                        sr.setStatus(StepRunStatus.SKIPPED);
                        sr.setCompletedAt(Instant.now());
                        stepRunDao.save(sr); // Reverted to not overwrite with mock null
                        recordStepMetrics(sr);
                    }
                }
                jobRun.setStatus(JobRunStatus.FAILURE);
            } else {
                jobRun.setStatus(JobRunStatus.SUCCESS);
            }

            jobRun.setCompletedAt(Instant.now());
            jobRunDao.save(jobRun); // Reverted to not overwrite with mock null
            
            // Record Job-level metrics immediately after durable terminal transition
            meterRegistry.counter("forgeflow_job_runs", "status", jobRun.getStatus().name()).increment();
            if (jobRun.getStartedAt() != null && jobRun.getCompletedAt() != null) {
                meterRegistry.timer("forgeflow_job_duration", "status", jobRun.getStatus().name())
                        .record(Duration.between(jobRun.getStartedAt(), jobRun.getCompletedAt()));
            }

            kafkaProducerService.publishJobRunCompleted(new JobRunCompletedEvent(jobRun.getId(), jobRun.getWorkflowRun().getId(), jobRun.getStatus()));

        } finally {
            if (locked) {
                if (stepExecutionEngine instanceof DockerExecutionService dockerEngine) {
                    dockerEngine.destroyWorkspace(jobRunId);
                }
                distributedLockService.unlock(lockKey);
            }
        }
    }

    private void recordStepMetrics(StepRun stepRun) {
        meterRegistry.counter("forgeflow_step_runs", "status", stepRun.getStatus().name()).increment();
        if (stepRun.getStartedAt() != null && stepRun.getCompletedAt() != null) {
            meterRegistry.timer("forgeflow_step_duration", "status", stepRun.getStatus().name())
                    .record(Duration.between(stepRun.getStartedAt(), stepRun.getCompletedAt()));
        }
    }
}