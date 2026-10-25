package forgeflow_api.orchestrator;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.worker.DistributedLockService;
import forgeflow_api.worker.DockerExecutionService;
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

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
@Testcontainers
class OrchestratorConcurrencyIntegrationTest {

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

    @Autowired
    private OrchestratorService orchestratorService;

    @MockitoSpyBean
    private DistributedLockService distributedLockService;

    @MockitoBean private JobRunDao jobRunDao;
    @MockitoBean private WorkflowJobDao workflowJobDao;
    @MockitoBean private WorkflowRunDao workflowRunDao;
    @MockitoBean private KafkaProducerService kafkaProducerService;
    @MockitoBean private DockerExecutionService dockerExecutionService;

    @BeforeEach
    void setUp() {
        reset(jobRunDao, workflowJobDao, workflowRunDao, distributedLockService, kafkaProducerService);
    }

    // Helper class to dynamically construct and mock DAGs for tests
    class MockGraph {
        WorkflowRun workflowRun = new WorkflowRun();
        Map<Long, WorkflowJob> jobs = new HashMap<>();
        Map<Long, JobRun> runs = new HashMap<>();

        MockGraph(Long workflowRunId) {
            workflowRun.setId(workflowRunId);
            workflowRun.setStatus(RunStatus.RUNNING);
        }

        void addJob(Long id, JobRunStatus status, Long... dependsOnIds) {
            WorkflowJob job = new WorkflowJob();
            job.setId(id);
            Set<WorkflowJob> deps = new HashSet<>();
            for (Long depId : dependsOnIds) {
                deps.add(jobs.get(depId));
            }
            job.setDependsOn(deps);
            jobs.put(id, job);

            JobRun run = new JobRun();
            run.setId(id);
            run.setWorkflowJob(job);
            run.setWorkflowRun(workflowRun);
            run.setStatus(status);
            runs.put(id, run);
        }

        void applyMocks() {
            when(jobRunDao.findByWorkflowRun(workflowRun)).thenReturn(new ArrayList<>(runs.values()));
            for (WorkflowJob job : jobs.values()) {
                List<WorkflowJob> downstreams = new ArrayList<>();
                for (WorkflowJob potentialDownstream : jobs.values()) {
                    if (potentialDownstream.getDependsOn().contains(job)) {
                        downstreams.add(potentialDownstream);
                    }
                }
                when(workflowJobDao.findByDependsOnContaining(job)).thenReturn(downstreams);
            }
            for (JobRun run : runs.values()) {
                when(jobRunDao.findById(run.getId())).thenReturn(Optional.of(run));
            }
        }
    }

    @Test
    void testLinearPipeline_BQueuedWhenASucceeds() {
        MockGraph graph = new MockGraph(100L);
        graph.addJob(1L, JobRunStatus.SUCCESS);
        graph.addJob(2L, JobRunStatus.PENDING, 1L);
        graph.addJob(3L, JobRunStatus.PENDING, 2L);
        graph.applyMocks();

        orchestratorService.processJobCompletion(1L);

        assertEquals(JobRunStatus.QUEUED, graph.runs.get(2L).getStatus());
        assertEquals(JobRunStatus.PENDING, graph.runs.get(3L).getStatus());
        verify(kafkaProducerService, times(1)).publishJobRunQueued(any());
        verify(jobRunDao, times(1)).save(graph.runs.get(2L));
    }

    @Test
    void testFanOut_BothQueued() {
        MockGraph graph = new MockGraph(101L);
        graph.addJob(1L, JobRunStatus.SUCCESS);
        graph.addJob(2L, JobRunStatus.PENDING, 1L);
        graph.addJob(3L, JobRunStatus.PENDING, 1L);
        graph.applyMocks();

        orchestratorService.processJobCompletion(1L);

        assertEquals(JobRunStatus.QUEUED, graph.runs.get(2L).getStatus());
        assertEquals(JobRunStatus.QUEUED, graph.runs.get(3L).getStatus());
        verify(kafkaProducerService, times(2)).publishJobRunQueued(any());
    }

    @Test
    void testFanIn_WaitState_ThenQueue() {
        MockGraph graph = new MockGraph(102L);
        graph.addJob(1L, JobRunStatus.SUCCESS);
        graph.addJob(2L, JobRunStatus.RUNNING);
        graph.addJob(3L, JobRunStatus.PENDING, 1L, 2L);
        graph.applyMocks();

        // A succeeds, but B is running -> C should WAIT
        orchestratorService.processJobCompletion(1L);
        assertEquals(JobRunStatus.PENDING, graph.runs.get(3L).getStatus());
        verify(kafkaProducerService, never()).publishJobRunQueued(any());

        // Now B succeeds -> C should QUEUE
        graph.runs.get(2L).setStatus(JobRunStatus.SUCCESS);
        orchestratorService.processJobCompletion(2L);
        assertEquals(JobRunStatus.QUEUED, graph.runs.get(3L).getStatus());
        verify(kafkaProducerService, times(1)).publishJobRunQueued(any());
    }

    @Test
    void testCascadeFailure_MarksDownstreamsSkipped() {
        MockGraph graph = new MockGraph(103L);
        graph.addJob(1L, JobRunStatus.FAILURE);
        graph.addJob(2L, JobRunStatus.PENDING, 1L);
        graph.addJob(3L, JobRunStatus.PENDING, 2L);
        graph.applyMocks();

        orchestratorService.processJobCompletion(1L);

        assertEquals(JobRunStatus.SKIPPED, graph.runs.get(2L).getStatus());
        assertEquals(JobRunStatus.SKIPPED, graph.runs.get(3L).getStatus());
        verify(kafkaProducerService, never()).publishJobRunQueued(any());
        
        // Evaluates to terminal failure
        assertEquals(RunStatus.FAILURE, graph.workflowRun.getStatus());
        verify(workflowRunDao, times(1)).save(graph.workflowRun);
    }

    @Test
    void testTerminalSuccess() {
        MockGraph graph = new MockGraph(104L);
        graph.addJob(1L, JobRunStatus.SUCCESS);
        graph.addJob(2L, JobRunStatus.SUCCESS, 1L);
        graph.applyMocks();

        orchestratorService.processJobCompletion(2L);

        assertEquals(RunStatus.SUCCESS, graph.workflowRun.getStatus());
        verify(workflowRunDao, times(1)).save(graph.workflowRun);
    }

    @Test
    void testDuplicateEvent_IsIgnored() {
        MockGraph graph = new MockGraph(105L);
        graph.addJob(1L, JobRunStatus.SUCCESS);
        graph.addJob(2L, JobRunStatus.QUEUED, 1L); // Already queued
        graph.applyMocks();

        orchestratorService.processJobCompletion(1L);

        // Should not requeue B
        verify(kafkaProducerService, never()).publishJobRunQueued(any());
        verify(jobRunDao, never()).save(any());
    }

    @Test
    void testParallelCompletionRace() throws InterruptedException {
        MockGraph graph = new MockGraph(106L);
        graph.addJob(1L, JobRunStatus.SUCCESS);
        graph.addJob(2L, JobRunStatus.SUCCESS);
        graph.addJob(3L, JobRunStatus.PENDING, 1L, 2L);
        graph.applyMocks();

        int threads = 2;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);

        executor.submit(() -> {
            try { latch.await(); orchestratorService.processJobCompletion(1L); } catch (Exception e) {}
        });
        executor.submit(() -> {
            try { latch.await(); orchestratorService.processJobCompletion(2L); } catch (Exception e) {}
        });

        latch.countDown();
        Thread.sleep(1000); // Allow threads to finish

        // Since Redis lock serializes access, one thread will queue it, the second will see it as QUEUED and ignore.
        verify(kafkaProducerService, atMost(1)).publishJobRunQueued(any());
    }
}