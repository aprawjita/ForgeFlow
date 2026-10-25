package forgeflow_api.worker;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.workflow.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@SpringBootTest
@Testcontainers
class WorkerStepOrchestrationIntegrationTest {

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static final GenericContainer<?> postgres = new GenericContainer<>("postgres:15-alpine")
            .withExposedPorts(5432)
            .withEnv("POSTGRES_USER", "testuser")
            .withEnv("POSTGRES_PASSWORD", "testpass")
            .withEnv("POSTGRES_DB", "forgeflow_db");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
        registry.add("spring.datasource.url", () -> String.format("jdbc:postgresql://%s:%d/forgeflow_db", postgres.getHost(), postgres.getFirstMappedPort()));
        registry.add("spring.datasource.username", () -> "testuser");
        registry.add("spring.datasource.password", () -> "testpass");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired private WorkerService workerService;

    @MockitoBean private JobRunDao jobRunDao;
    @MockitoBean private StepRunDao stepRunDao;
    @MockitoBean private WorkflowJobDao workflowJobDao;
    @MockitoBean private WorkflowRunDao workflowRunDao;

    @MockitoSpyBean private DistributedLockService distributedLockService;
    @MockitoBean private StepExecutionEngine stepExecutionEngine;
    @MockitoBean private KafkaProducerService kafkaProducerService;

    private JobRun testJobRun;
    private Map<Long, StepRun> stepRunDb;
    private long stepRunIdCounter;

    @BeforeEach
    void setup() {
        stepRunDb = new HashMap<>();
        stepRunIdCounter = 1;

        WorkflowRun wRun = new WorkflowRun();
        wRun.setId(500L);

        WorkflowJob wJob = new WorkflowJob();
        wJob.setId(600L);
        wJob.setSteps(List.of(
                WorkflowStep.builder().workflowJob(wJob).executionOrder(1).name("Step 1").command("echo 1").build(),
                WorkflowStep.builder().workflowJob(wJob).executionOrder(2).name("Step 2").command("echo 2").build(),
                WorkflowStep.builder().workflowJob(wJob).executionOrder(3).name("Step 3").command("echo 3").build()
        ));

        testJobRun = new JobRun();
        testJobRun.setId(100L);
        testJobRun.setWorkflowRun(wRun);
        testJobRun.setWorkflowJob(wJob);
        testJobRun.setStatus(JobRunStatus.QUEUED);
        testJobRun.setStartedAt(Instant.now());

        when(jobRunDao.findById(100L)).thenReturn(Optional.of(testJobRun));
        when(jobRunDao.save(any(JobRun.class))).thenAnswer(i -> i.getArgument(0));

        when(stepRunDao.save(any(StepRun.class))).thenAnswer(i -> {
            StepRun sr = i.getArgument(0);
            if (sr.getId() == null) {
                sr.setId(stepRunIdCounter++);
            }
            stepRunDb.put(sr.getId(), sr);
            return sr;
        });

        when(stepRunDao.findByJobRun(any(JobRun.class))).thenAnswer(i -> {
            JobRun jr = i.getArgument(0);
            return stepRunDb.values().stream()
                    .filter(sr -> sr.getJobRun().getId().equals(jr.getId()))
                    .sorted(Comparator.comparing(sr -> sr.getWorkflowStep().getExecutionOrder()))
                    .toList();
        });
    }

    private List<StepRun> getSortedStepRuns() {
        return stepRunDao.findByJobRun(testJobRun);
    }

    @Test
    void test1_AllSuccessful() {
        when(stepExecutionEngine.executeStep(any(), any(), any())).thenReturn(0); 

        workerService.executeJob(testJobRun.getId());

        assertEquals(JobRunStatus.SUCCESS, testJobRun.getStatus());

        List<StepRun> stepRuns = getSortedStepRuns();
        assertEquals(3, stepRuns.size());
        assertEquals(StepRunStatus.SUCCESS, stepRuns.get(0).getStatus());
        assertEquals(StepRunStatus.SUCCESS, stepRuns.get(1).getStatus());
        assertEquals(StepRunStatus.SUCCESS, stepRuns.get(2).getStatus());
        
        verify(kafkaProducerService, times(1)).publishJobRunCompleted(any());
    }

    @Test
    void test2_FirstStepFails() {
        when(stepExecutionEngine.executeStep(any(), argThat(sr -> sr != null && sr.getWorkflowStep().getExecutionOrder() == 1), any())).thenReturn(1);

        workerService.executeJob(testJobRun.getId());

        assertEquals(JobRunStatus.FAILURE, testJobRun.getStatus());

        List<StepRun> stepRuns = getSortedStepRuns();
        assertEquals(StepRunStatus.FAILURE, stepRuns.get(0).getStatus());
        assertEquals(StepRunStatus.SKIPPED, stepRuns.get(1).getStatus());
        assertEquals(StepRunStatus.SKIPPED, stepRuns.get(2).getStatus());

        verify(stepExecutionEngine, times(1)).executeStep(any(), any(), any());
    }

    @Test
    void test3_MiddleStepFails() {
        when(stepExecutionEngine.executeStep(any(), argThat(sr -> sr != null && sr.getWorkflowStep().getExecutionOrder() == 1), any())).thenReturn(0);
        when(stepExecutionEngine.executeStep(any(), argThat(sr -> sr != null && sr.getWorkflowStep().getExecutionOrder() == 2), any())).thenReturn(1);

        workerService.executeJob(testJobRun.getId());

        assertEquals(JobRunStatus.FAILURE, testJobRun.getStatus());

        List<StepRun> stepRuns = getSortedStepRuns();
        assertEquals(StepRunStatus.SUCCESS, stepRuns.get(0).getStatus());
        assertEquals(StepRunStatus.FAILURE, stepRuns.get(1).getStatus());
        assertEquals(StepRunStatus.SKIPPED, stepRuns.get(2).getStatus());

        verify(stepExecutionEngine, times(2)).executeStep(any(), any(), any());
    }

    @Test
    void test4_DuplicateInvocationIdempotency() {
        when(stepExecutionEngine.executeStep(any(), any(), any())).thenReturn(0);

        workerService.executeJob(testJobRun.getId());
        workerService.executeJob(testJobRun.getId()); 

        List<StepRun> stepRuns = getSortedStepRuns();
        assertEquals(3, stepRuns.size());
        
        verify(stepExecutionEngine, times(3)).executeStep(any(), any(), any());
        verify(kafkaProducerService, times(1)).publishJobRunCompleted(any());
    }
}