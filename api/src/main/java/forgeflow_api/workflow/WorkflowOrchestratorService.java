package forgeflow_api.workflow;

import forgeflow_api.kafka.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class WorkflowOrchestratorService {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowOrchestratorService.class);

    private final WorkflowRunDao workflowRunDao;
    private final WorkflowJobDao workflowJobDao;
    private final JobRunDao jobRunDao;
    private final KafkaProducerService kafkaProducerService;

    @Transactional
    public void orchestrateWorkflowRun(Long workflowRunId) {
        evaluateDagProgression(workflowRunId);
    }

    @Transactional
    public void evaluateDagProgression(Long workflowRunId) {
        WorkflowRun run = workflowRunDao.findById(workflowRunId).orElse(null);
        if (run == null || run.getStatus() == RunStatus.SUCCESS || run.getStatus() == RunStatus.FAILURE) {
            return;
        }

        List<WorkflowJob> allJobs = workflowJobDao.findByWorkflow(run.getWorkflow());
        List<JobRun> existingJobRuns = jobRunDao.findByWorkflowRun(run);

        boolean allJobsCompleted = true;
        boolean anyJobFailed = false;

        for (WorkflowJob job : allJobs) {
            Optional<JobRun> existingRunOpt = existingJobRuns.stream()
                    .filter(jr -> jr.getWorkflowJob().getId().equals(job.getId()))
                    .findFirst();

            if (existingRunOpt.isPresent()) {
                JobRun jr = existingRunOpt.get();
                if (jr.getStatus() != JobRunStatus.SUCCESS && jr.getStatus() != JobRunStatus.FAILURE && jr.getStatus() != JobRunStatus.BLOCKED) {
                    allJobsCompleted = false;
                }
                if (jr.getStatus() == JobRunStatus.FAILURE || jr.getStatus() == JobRunStatus.BLOCKED) {
                    anyJobFailed = true;
                }
                continue;
            }

            allJobsCompleted = false;
            evaluateJobEligibility(run, job, existingJobRuns);
        }

        if (allJobsCompleted) {
            run.setStatus(anyJobFailed ? RunStatus.FAILURE : RunStatus.SUCCESS);
            workflowRunDao.save(run);
        }
    }

    private void evaluateJobEligibility(WorkflowRun run, WorkflowJob job, List<JobRun> existingJobRuns) {
        if (job.getDependsOn() == null || job.getDependsOn().isEmpty()) {
            queueJob(run, job);
            return;
        }

        boolean allDependenciesSuccessful = true;
        boolean anyDependencyFailed = false;

        for (WorkflowJob dependency : job.getDependsOn()) {
            Optional<JobRun> depRunOpt = existingJobRuns.stream()
                    .filter(jr -> jr.getWorkflowJob().getId().equals(dependency.getId()))
                    .findFirst();

            if (depRunOpt.isEmpty()) {
                allDependenciesSuccessful = false;
                continue;
            }

            JobRunStatus depStatus = depRunOpt.get().getStatus();
            if (depStatus == JobRunStatus.FAILURE || depStatus == JobRunStatus.BLOCKED) {
                anyDependencyFailed = true;
            } else if (depStatus != JobRunStatus.SUCCESS) {
                allDependenciesSuccessful = false;
            }
        }

        if (anyDependencyFailed) {
            blockJob(run, job);
        } else if (allDependenciesSuccessful) {
            queueJob(run, job);
        }
    }

    private void queueJob(WorkflowRun run, WorkflowJob job) {
        JobRun jobRun = JobRun.builder()
                .workflowRun(run)
                .workflowJob(job)
                .status(JobRunStatus.QUEUED)
                .build();
        jobRun = jobRunDao.save(jobRun);

        try {
            kafkaProducerService.publishJobRunQueued(jobRun.getId());
        } catch (Exception e) {
            jobRun.setStatus(JobRunStatus.FAILURE);
            jobRunDao.save(jobRun);
            run.setStatus(RunStatus.FAILURE);
            workflowRunDao.save(run);
        }
    }

    private void blockJob(WorkflowRun run, WorkflowJob job) {
        JobRun jobRun = JobRun.builder()
                .workflowRun(run)
                .workflowJob(job)
                .status(JobRunStatus.BLOCKED)
                .build();
        jobRunDao.save(jobRun);
    }
}