package forgeflow_api.orchestrator;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.worker.DistributedLockService;
import forgeflow_api.workflow.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest
@Testcontainers
class OrchestratorMetricsIntegrationTest {

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

    @Autowired private OrchestratorService orchestratorService;
    @Autowired private MeterRegistry meterRegistry;

    @MockitoBean private JobRunDao jobRunDao;
    @MockitoBean private WorkflowRunDao workflowRunDao;
    @MockitoBean private WorkflowJobDao workflowJobDao;
    @MockitoBean private DistributedLockService distributedLockService;
    @MockitoBean private KafkaProducerService kafkaProducerService;

    private WorkflowRun workflowRun;
    private JobRun jobRun1;
    private JobRun jobRun2;

    @BeforeEach
    void setup() {
        meterRegistry.clear();

        when(distributedLockService.tryLock(anyString())).thenReturn(true);

        workflowRun = new WorkflowRun();
        workflowRun.setId(100L);
        workflowRun.setStatus(RunStatus.RUNNING);
        workflowRun.setStartedAt(Instant.now().minusSeconds(120));

        WorkflowJob wJob1 = new WorkflowJob(); wJob1.setId(10L); wJob1.setDependsOn(Collections.emptySet());
        WorkflowJob wJob2 = new WorkflowJob(); wJob2.setId(20L); wJob2.setDependsOn(Collections.emptySet());

        jobRun1 = new JobRun();
        jobRun1.setId(1L);
        jobRun1.setWorkflowRun(workflowRun);
        jobRun1.setWorkflowJob(wJob1);
        jobRun1.setStatus(JobRunStatus.SUCCESS);

        jobRun2 = new JobRun();
        jobRun2.setId(2L);
        jobRun2.setWorkflowRun(workflowRun);
        jobRun2.setWorkflowJob(wJob2);
        jobRun2.setStatus(JobRunStatus.SUCCESS);

        when(jobRunDao.findById(1L)).thenReturn(Optional.of(jobRun1));
        when(jobRunDao.findById(2L)).thenReturn(Optional.of(jobRun2));
        when(jobRunDao.findByWorkflowRun(workflowRun)).thenReturn(List.of(jobRun1, jobRun2));
        when(workflowJobDao.findByDependsOnContaining(any())).thenReturn(Collections.emptyList());
        
        when(workflowRunDao.save(any(WorkflowRun.class))).thenAnswer(i -> i.getArgument(0));
        when(jobRunDao.save(any(JobRun.class))).thenAnswer(i -> i.getArgument(0));
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
    void test1_WorkflowSuccess_MissingCompletedAt() {
        // Both jobs SUCCESS, completedAt is null (current production behavior)
        orchestratorService.processJobCompletion(1L);

        assertEquals(1.0, getCounter("forgeflow_workflow_runs", "SUCCESS"));
        assertEquals(0.0, getTimerCount("forgeflow_workflow_duration", "SUCCESS")); // Should safely ignore duration
        assertEquals(RunStatus.SUCCESS, workflowRun.getStatus());
    }

    @Test
    void test2_WorkflowFailure_WithTimestamps() {
        jobRun2.setStatus(JobRunStatus.FAILURE); // Mixed outcome: One failure fails the workflow
        workflowRun.setCompletedAt(Instant.now()); 

        orchestratorService.processJobCompletion(2L);

        assertEquals(1.0, getCounter("forgeflow_workflow_runs", "FAILURE"));
        assertEquals(1.0, getTimerCount("forgeflow_workflow_duration", "FAILURE"));
        assertEquals(RunStatus.FAILURE, workflowRun.getStatus());
    }

    @Test
    void test3_DuplicateCompletion_Idempotency() {
        orchestratorService.processJobCompletion(1L);
        assertEquals(1.0, getCounter("forgeflow_workflow_runs", "SUCCESS"));

        // Duplicate processing path
        orchestratorService.processJobCompletion(1L);
        assertEquals(1.0, getCounter("forgeflow_workflow_runs", "SUCCESS"), "Counter must remain exactly 1");
    }
    
    @Test
    void test4_PartialCompletion_NoMetrics() {
        jobRun2.setStatus(JobRunStatus.RUNNING); // DAG not terminal

        orchestratorService.processJobCompletion(1L);

        assertEquals(0.0, getCounter("forgeflow_workflow_runs", "SUCCESS"));
        assertEquals(0.0, getCounter("forgeflow_workflow_runs", "FAILURE"));
        assertEquals(RunStatus.RUNNING, workflowRun.getStatus());
    }
}