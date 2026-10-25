package forgeflow_api.worker;

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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

import forgeflow_api.kafka.*;
import forgeflow_api.workflow.*;
import forgeflow_api.artifact.ArtifactStorageService;
import forgeflow_api.worker.logs.LogStorageService;
import forgeflow_api.repository.ConnectedRepository;

@SpringBootTest
@Testcontainers
class WorkerServiceConcurrencyIntegrationTest {

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

    @MockitoSpyBean private DistributedLockService distributedLockService;
    @MockitoBean private JobRunDao jobRunDao;
    @MockitoBean private StepRunDao stepRunDao;
    @MockitoBean private StepExecutionEngine stepExecutionEngine;
    @MockitoBean private KafkaProducerService kafkaProducerService;
    @MockitoBean private WorkspaceManager workspaceManager;
    @MockitoBean private RepositoryCheckoutService repositoryCheckoutService;
    @MockitoBean private ArtifactStorageService artifactStorageService;
    @MockitoBean private LogStorageService logStorageService;

    @BeforeEach
    void setUp() throws Exception {
        reset(jobRunDao, stepRunDao, stepExecutionEngine, kafkaProducerService, workspaceManager, repositoryCheckoutService, logStorageService);

        Path dummyPath = Paths.get("D:/ForgeFlow/workspace-test");
        when(workspaceManager.createWorkspace(any())).thenReturn(dummyPath);
        when(logStorageService.getLogLocation(any(), any())).thenReturn("logs/test-path");
        when(stepRunDao.save(any())).thenAnswer(invocation -> invocation.getArgument(0));
        
        when(stepExecutionEngine.executeStep(any(), any(), any())).thenReturn(0);
    }

    private JobRun createValidJobRun(Long id, JobRunStatus status) {
        ConnectedRepository repo = new ConnectedRepository();
        Workflow workflow = new Workflow();
        workflow.setConnectedRepository(repo);

        WorkflowRun run = new WorkflowRun();
        run.setId(99L);
        run.setCommitSha("abc1234");
        run.setWorkflow(workflow);

        WorkflowJob job = new WorkflowJob();
        job.setConsumes(new ArrayList<>());
        job.setProduces(new ArrayList<>());
        
        WorkflowStep step = new WorkflowStep();
        step.setName("Build Step");
        step.setCommand("echo test");
        step.setExecutionOrder(1);
        
        ArrayList<WorkflowStep> steps = new ArrayList<>();
        steps.add(step);
        job.setSteps(steps);

        JobRun jobRun = new JobRun();
        jobRun.setId(id);
        jobRun.setStatus(status);
        jobRun.setWorkflowRun(run);
        jobRun.setWorkflowJob(job);
        
        return jobRun;
    }

    @Test
    void testConcurrentExecution_ExactlyOneWorkerExecutes_And_ExactlyOneEventEmitted() throws InterruptedException {
        JobRun queuedJob = createValidJobRun(1L, JobRunStatus.QUEUED);
        when(jobRunDao.findById(1L)).thenReturn(Optional.of(queuedJob));

        int threadCount = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger successExecutions = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    workerService.executeJob(1L);
                    successExecutions.incrementAndGet();
                } catch (Exception e) {
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();

        verify(stepExecutionEngine, times(1)).executeStep(argThat(jr -> jr != null && jr.getId().equals(1L)), any(), any());
        verify(kafkaProducerService, times(1)).publishJobRunCompleted(any());
        assertEquals(JobRunStatus.SUCCESS, queuedJob.getStatus());
    }

    @Test
    void testIdempotency_SuccessState_IsIgnored() {
        JobRun successJob = createValidJobRun(2L, JobRunStatus.SUCCESS);
        when(jobRunDao.findById(2L)).thenReturn(Optional.of(successJob));
        workerService.executeJob(2L);
        verify(stepExecutionEngine, never()).executeStep(any(), any(), any());
    }

    @Test
    void testIdempotency_RunningState_IsIgnored() {
        JobRun runningJob = createValidJobRun(3L, JobRunStatus.RUNNING);
        when(jobRunDao.findById(3L)).thenReturn(Optional.of(runningJob));
        workerService.executeJob(3L);
        verify(stepExecutionEngine, never()).executeStep(any(), any(), any());
    }

    @Test
    void testIdempotency_FailureState_IsIgnored() {
        JobRun failJob = createValidJobRun(4L, JobRunStatus.FAILURE);
        when(jobRunDao.findById(4L)).thenReturn(Optional.of(failJob));
        workerService.executeJob(4L);
        verify(stepExecutionEngine, never()).executeStep(any(), any(), any());
    }

    @Test
    void testLockRelease_OnSuccess() {
        JobRun job = createValidJobRun(5L, JobRunStatus.QUEUED);
        when(jobRunDao.findById(5L)).thenReturn(Optional.of(job));

        workerService.executeJob(5L);

        boolean lockAcquired = distributedLockService.tryLock("forgeflow:lock:jobrun:5");
        assertTrue(lockAcquired, "Lock must be available after successful execution");
        if(lockAcquired) distributedLockService.unlock("forgeflow:lock:jobrun:5");
    }

    @Test
    void testLockRelease_OnException() throws Exception {
        JobRun job = createValidJobRun(6L, JobRunStatus.QUEUED);
        when(jobRunDao.findById(6L)).thenReturn(Optional.of(job));
        
        when(stepExecutionEngine.executeStep(any(), any(), any()))
            .thenThrow(new RuntimeException("Simulated Docker Crash"));

        workerService.executeJob(6L);

        boolean lockAcquired = distributedLockService.tryLock("forgeflow:lock:jobrun:6");
        assertTrue(lockAcquired, "Lock must be available after an exception");
        if (lockAcquired) {
            distributedLockService.unlock("forgeflow:lock:jobrun:6");
        }
    }

    @Test
    void testRedisUnavailable_FailsClosed() {
        doThrow(new RuntimeException("Simulated Redis Failure")).when(distributedLockService).tryLock(anyString());

        assertThrows(RuntimeException.class, () -> workerService.executeJob(99L));

        verify(jobRunDao, never()).save(any());
        verify(stepExecutionEngine, never()).executeStep(any(), any(), any());
    }
}