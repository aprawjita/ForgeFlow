package forgeflow_api.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

@Component
public class WorkspaceManager {

    private static final Logger logger = LoggerFactory.getLogger(WorkspaceManager.class);

    public Path createWorkspace(Long jobRunId) {
        try {
            Path workspace = Files.createTempDirectory("forgeflow-workspace-job-" + jobRunId + "-");
            logger.info("Created temporary workspace at {} for JobRun ID: {}", workspace.toAbsolutePath(), jobRunId);
            return workspace;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create workspace for JobRun ID: " + jobRunId, e);
        }
    }

    public void cleanupWorkspace(Path workspace) {
        if (workspace == null || !Files.exists(workspace)) return;
        try {
            Files.walk(workspace)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            logger.info("Cleaned up workspace at {}", workspace.toAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to clean up workspace at {}: {}", workspace.toAbsolutePath(), e.getMessage());
        }
    }
}