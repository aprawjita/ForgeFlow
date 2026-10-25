package forgeflow_api.worker;

import forgeflow_api.workflow.JobRun;
import forgeflow_api.workflow.StepRun;
import java.util.concurrent.atomic.AtomicLong;

public interface StepExecutionEngine {
    int executeStep(JobRun jobRun, StepRun stepRun, AtomicLong logSequence);
}