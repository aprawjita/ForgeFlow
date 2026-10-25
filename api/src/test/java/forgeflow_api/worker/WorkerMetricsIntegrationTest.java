package forgeflow_api.worker;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.workflow.*;
import io.micrometer.core.instrument.MeterRegistry;
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
class WorkerMetricsIntegrationTest {

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
    @Autowired private MeterRegistry meterRegistry;

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
        meterRegistry.clear(); // Isolate test state
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
        testJobRun.setStartedAt(Instant.now().minusSeconds(60));

        when(jobRunDao.findById(100L)).thenReturn(Optional.of(testJobRun));
        when(jobRunDao.save(any(JobRun.class))).thenAnswer(i -> i.getArgument(0));

        when(stepRunDao.save(any(StepRun.class))).thenAnswer(i -> {
            StepRun sr = i.getArgument(0);
            if (sr.getId() == null) {
                sr.setId(stepRunIdCounter++);
            }
            if (sr.getStartedAt() == null) {
                sr.setStartedAt(Instant.now().minusSeconds(10)); // Simulate Hibernate @CreationTimestamp
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

    private double getCounter(String name, String status) {
        var counter = meterRegistry.find(name).tag("status", status).counter();
        return counter != null ? counter.count() : 0.0;
    }

    private double getTimerCount(String name, String status) {
        var timer = meterRegistry.find(name).tag("status", status).timer();
        return timer != null ? timer.count() : 0.0;
    }

    @Test
    void test1_SuccessMetrics() {
        when(stepExecutionEngine.executeStep(any(), any(), any())).thenReturn(0); 
        workerService.executeJob(testJobRun.getId());

        assertEquals(1.0, getCounter("forgeflow_job_runs", "SUCCESS"));
        assertEquals(1.0, getTimerCount("forgeflow_job_duration", "SUCCESS"));

        assertEquals(3.0, getCounter("forgeflow_step_runs", "SUCCESS"));
        assertEquals(3.0, getTimerCount("forgeflow_step_duration", "SUCCESS"));
        
        assertEquals(0.0, getCounter("forgeflow_step_runs", "FAILURE"));
        assertEquals(0.0, getCounter("forgeflow_step_runs", "SKIPPED"));
    }

    @Test
    void test2_FailureAndSkippedMetrics() {
        when(stepExecutionEngine.executeStep(any(), argThat(sr -> sr != null && sr.getWorkflowStep().getExecutionOrder() == 1), any())).thenReturn(0);
        when(stepExecutionEngine.executeStep(any(), argThat(sr -> sr != null && sr.getWorkflowStep().getExecutionOrder() == 2), any())).thenReturn(1);

        workerService.executeJob(testJobRun.getId());

        assertEquals(1.0, getCounter("forgeflow_job_runs", "FAILURE"));
        assertEquals(1.0, getTimerCount("forgeflow_job_duration", "FAILURE"));

        assertEquals(1.0, getCounter("forgeflow_step_runs", "SUCCESS"));
        assertEquals(1.0, getCounter("forgeflow_step_runs", "FAILURE"));
        assertEquals(1.0, getCounter("forgeflow_step_runs", "SKIPPED"));

        assertEquals(1.0, getTimerCount("forgeflow_step_duration", "SUCCESS"));
        assertEquals(1.0, getTimerCount("forgeflow_step_duration", "FAILURE"));
        assertEquals(1.0, getTimerCount("forgeflow_step_duration", "SKIPPED"));
    }

    @Test
    void test3_DuplicateInvocationIdempotency() {
        when(stepExecutionEngine.executeStep(any(), any(), any())).thenReturn(0);

        workerService.executeJob(testJobRun.getId());
        workerService.executeJob(testJobRun.getId()); // Second invocation should hit idempotency wall

        assertEquals(1.0, getCounter("forgeflow_job_runs", "SUCCESS"), "Duplicate invocation should not double count");
        assertEquals(3.0, getCounter("forgeflow_step_runs", "SUCCESS"), "Duplicate invocation should not double count");
    }
}