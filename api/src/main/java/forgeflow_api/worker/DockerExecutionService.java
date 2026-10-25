package forgeflow_api.worker;

import forgeflow_api.worker.logs.LogEvent;
import forgeflow_api.worker.logs.LogMaskingService;
import forgeflow_api.worker.logs.LogStorageService;
import forgeflow_api.worker.logs.RedisLogPublisher;
import forgeflow_api.workflow.JobRun;
import forgeflow_api.workflow.StepRun;
import forgeflow_api.workflow.WorkflowStep;
import forgeflow_api.workflow.secrets.SecureEnvFileBuilder;
import forgeflow_api.workflow.secrets.WorkflowSecretProvider;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class DockerExecutionService implements StepExecutionEngine {

    private static final Logger logger = LoggerFactory.getLogger(DockerExecutionService.class);
    private final LogStorageService logStorageService;
    private final RedisLogPublisher redisLogPublisher;
    private final LogMaskingService logMaskingService;
    private final WorkflowSecretProvider workflowSecretProvider;

    private final Path hostWorkspaceBase = Paths.get("/tmp/forgeflow-workspaces");

    @Override
    public int executeStep(JobRun jobRun, StepRun stepRun, AtomicLong logSequence) {
        WorkflowStep step = stepRun.getWorkflowStep();
        Long workflowRunId = jobRun.getWorkflowRun().getId();
        Long jobRunId = jobRun.getId();
        String containerName = "forgeflow_job_" + jobRunId;
        
        Path workspace = hostWorkspaceBase.resolve(jobRunId.toString());
        try {
            if (!Files.exists(workspace)) Files.createDirectories(workspace);
        } catch (Exception e) {
            logger.error("Failed to create host workspace directory", e);
            return -1;
        }

        if (!isContainerRunning(containerName) && !startWorkspaceContainer(containerName, workspace)) {
            return -1; 
        }

        int exitCode = -1;
        
        // 1. Resolve Vault Secrets
        Map<String, String> resolvedSecrets = Map.of();
        try {
            Map<String, String> requiredSecrets = step.getWorkflowJob().getSecrets();
            if (requiredSecrets != null && !requiredSecrets.isEmpty()) {
                Long repositoryId = step.getWorkflowJob().getWorkflow().getConnectedRepository().getId();
                resolvedSecrets = workflowSecretProvider.resolveSecrets(repositoryId, requiredSecrets);
            }
        } catch (Exception e) {
            logger.error("Failed to resolve Vault secrets for JobRun {}", jobRunId, e);
            return -1;
        }

        // 2. Extract execution-scoped secrets for masking
        Collection<String> secretValuesToMask = resolvedSecrets.values();

        try (SecureEnvFileBuilder envFile = new SecureEnvFileBuilder(resolvedSecrets);
             OutputStream logStream = logStorageService.getLogOutputStream(workflowRunId, jobRunId)) {
            
            String stepHeader = String.format("\n--- Executing Step: %s ---\n", step.getName());
            synchronized (logStream) {
                logStream.write(stepHeader.getBytes(StandardCharsets.UTF_8));
                logStream.flush();
            }
            
            ProcessBuilder pb = new ProcessBuilder(
                    "docker", "exec", 
                    "--env-file", envFile.getEnvFilePath().toString(), 
                    containerName,
                    "/bin/sh", "-c", step.getCommand()
            );
            
            Process process = pb.start();

            // 3. Inject masking targets into the stream pumps
            Thread outThread = new Thread(() -> streamPump(process.getInputStream(), "STDOUT", jobRun, stepRun, logSequence, logStream, secretValuesToMask));
            Thread errThread = new Thread(() -> streamPump(process.getErrorStream(), "STDERR", jobRun, stepRun, logSequence, logStream, secretValuesToMask));
            
            outThread.start();
            errThread.start();

            boolean finished = process.waitFor(5, TimeUnit.MINUTES);
            
            outThread.join(5000);
            errThread.join(5000);

            if (!finished) {
                process.destroyForcibly();
                synchronized (logStream) {
                    logStream.write("\n[ERROR] Step execution timed out after 5 minutes.".getBytes(StandardCharsets.UTF_8));
                    logStream.flush();
                }
                return -1;
            }
            exitCode = process.exitValue();
            
        } catch (Exception e) {
            logger.error("Docker step execution failed for container {}", containerName, e);
        }

        return exitCode;
    }

    private void streamPump(InputStream is, String streamType, JobRun jobRun, StepRun stepRun, AtomicLong sequence, OutputStream durableLog, Collection<String> secrets) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                
                // MASKING BOUNDARY: Intercepted before ANY downstream consumption
                String safeLine = logMaskingService.mask(line, secrets);
                
                byte[] bytes = (safeLine + "\n").getBytes(StandardCharsets.UTF_8);
                synchronized (durableLog) {
                    durableLog.write(bytes);
                    durableLog.flush();
                }
                
                LogEvent event = LogEvent.builder()
                        .jobRunId(jobRun.getId())
                        .stepRunId(stepRun.getId())
                        .sequence(sequence.incrementAndGet())
                        .timestamp(Instant.now())
                        .stream(streamType)
                        .message(safeLine) // Safely transmit to Redis/WebSocket
                        .build();
                redisLogPublisher.publish(event);
            }
        } catch (Exception e) {
            logger.warn("Error reading Docker stream {}", streamType, e);
        }
    }

    // ... destroyWorkspace, isContainerRunning, startWorkspaceContainer remain unchanged
    public void destroyWorkspace(Long jobRunId) {
        String containerName = "forgeflow_job_" + jobRunId;
        try {
            ProcessBuilder pb = new ProcessBuilder("docker", "kill", containerName);
            pb.start().waitFor(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.warn("Failed to kill container {} (it may have already died)", containerName);
        }

        try {
            Path workspace = hostWorkspaceBase.resolve(jobRunId.toString()).normalize();
            if (workspace.startsWith(hostWorkspaceBase.normalize()) && Files.exists(workspace)) {
                Files.walk(workspace).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
            }
        } catch (Exception e) {
            logger.error("Failed to fully clean up host workspace directory for JobRun: {}", jobRunId, e);
        }
    }

    private boolean isContainerRunning(String containerName) {
        try {
            ProcessBuilder pb = new ProcessBuilder("docker", "ps", "-q", "-f", "name=" + containerName);
            Process process = pb.start();
            process.waitFor(5, TimeUnit.SECONDS);
            return process.getInputStream().read() != -1;
        } catch (Exception e) { return false; }
    }

    private boolean startWorkspaceContainer(String containerName, Path workspace) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                    "docker", "run", "-d", "--rm", "--name", containerName,
                    "-v", workspace.toAbsolutePath() + ":/workspace", "-w", "/workspace",
                    "--memory", "512m", "--cpus", "1.0", "ubuntu:22.04", "sleep", "infinity"
            );
            Process process = pb.start();
            if (!process.waitFor(30, TimeUnit.SECONDS) || process.exitValue() != 0) return false;
            return true;
        } catch (Exception e) { return false; }
    }
}