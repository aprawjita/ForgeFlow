package forgeflow_api.worker;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.worker.logs.LogStorageService;
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

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@SpringBootTest
@Testcontainers
class DockerFilesystemIntegrationTest {

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

    // Use a Spy for the Docker Engine to inject runtime exceptions
    @MockitoSpyBean private DockerExecutionService dockerExecutionService;

    @MockitoBean private JobRunDao jobRunDao;
    @MockitoBean private StepRunDao stepRunDao;
    @MockitoBean private WorkflowJobDao workflowJobDao;
    @MockitoBean private WorkflowRunDao workflowRunDao;

    @MockitoSpyBean private DistributedLockService distributedLockService;
    @MockitoBean private KafkaProducerService kafkaProducerService;
    @MockitoBean private LogStorageService logStorageService;

    private Map<Long, JobRun> jobRunDb;
    private Map<Long, StepRun> stepRunDb;
    private long stepRunIdCounter;

    private final Path hostWorkspaceBase = Paths.get("/tmp/forgeflow-workspaces");

    @BeforeEach
    void setup() throws Exception {
        jobRunDb = new HashMap<>();
        stepRunDb = new HashMap<>();
        stepRunIdCounter = 1;

        when(logStorageService.getLogOutputStream(any(), any())).thenReturn(new ByteArrayOutputStream());

        when(jobRunDao.findById(any())).thenAnswer(i -> Optional.ofNullable(jobRunDb.get(i.getArgument(0))));
        when(jobRunDao.save(any(JobRun.class))).thenAnswer(i -> {
            JobRun jr = i.getArgument(0);
            jobRunDb.put(jr.getId(), jr);
            return jr;
        });

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

    private JobRun createJobRun(Long jobId, List<WorkflowStep> steps) {
        WorkflowRun wRun = new WorkflowRun();
        wRun.setId(500L);

        WorkflowJob wJob = new WorkflowJob();
        wJob.setId(600L);
        wJob.setName("docker-job");
        steps.forEach(s -> s.setWorkflowJob(wJob));
        wJob.setSteps(steps);

        JobRun jobRun = new JobRun();
        jobRun.setId(jobId);
        jobRun.setWorkflowRun(wRun);
        jobRun.setWorkflowJob(wJob);
        jobRun.setStatus(JobRunStatus.QUEUED);
        jobRun.setStartedAt(Instant.now());
        
        jobRunDb.put(jobId, jobRun);
        return jobRun;
    }

    private List<StepRun> getSortedStepRuns(JobRun jobRun) {
        List<StepRun> steps = new ArrayList<>(stepRunDao.findByJobRun(jobRun));
        steps.sort(Comparator.comparing(s -> s.getWorkflowStep().getExecutionOrder()));
        return steps;
    }

    @Test
    void test1_SuccessfulJobRun_CleansUpWorkspace() {
        JobRun testJob = createJobRun(100L, List.of(
                WorkflowStep.builder().executionOrder(1).name("Write").command("echo 'hello' > /workspace/test.txt").build()
        ));

        workerService.executeJob(testJob.getId());

        assertEquals(JobRunStatus.SUCCESS, jobRunDao.findById(100L).get().getStatus());
        Path workspace = hostWorkspaceBase.resolve("100");
        assertFalse(Files.exists(workspace), "Workspace directory must be deleted after success");
    }

    @Test
    void test2_FailedJobRun_CleansUpWorkspace() {
        JobRun testJob = createJobRun(101L, List.of(
                WorkflowStep.builder().executionOrder(1).name("Fail").command("exit 1").build()
        ));

        workerService.executeJob(testJob.getId());

        assertEquals(JobRunStatus.FAILURE, jobRunDao.findById(101L).get().getStatus());
        Path workspace = hostWorkspaceBase.resolve("101");
        assertFalse(Files.exists(workspace), "Workspace directory must be deleted after failure");
    }

    @Test
    void test3_DockerException_CleansUpWorkspace_AndFailsSafely() {
        JobRun testJob = createJobRun(102L, List.of(
                WorkflowStep.builder().executionOrder(1).name("Throw").command("echo 'run'").build()
        ));

        // Added the 3rd any() for the AtomicLong sequence
        doThrow(new RuntimeException("Simulated Docker Catastrophe"))
                .when(dockerExecutionService).executeStep(any(), any(), any());

        workerService.executeJob(testJob.getId());

        assertEquals(JobRunStatus.FAILURE, jobRunDao.findById(102L).get().getStatus());
        Path workspace = hostWorkspaceBase.resolve("102");
        assertFalse(Files.exists(workspace), "Workspace directory must be deleted even if Docker Java code throws an exception");
    }

    @Test
    void test4_IdempotentCleanup_DoesNotThrow() {
        // Manually wiping a path that doesn't exist to ensure no exceptions bubble up
        assertDoesNotThrow(() -> dockerExecutionService.destroyWorkspace(999L));
        // Wiping it twice to prove idempotency
        assertDoesNotThrow(() -> dockerExecutionService.destroyWorkspace(999L));
    }
}