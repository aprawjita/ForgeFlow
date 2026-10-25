package forgeflow_api.workflow.secrets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;

public class SecureEnvFileBuilder implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SecureEnvFileBuilder.class);
    
    private static final String PREFIX = "ff-env-secret-";
    private static final String SUFFIX = ".env";

    private final Path envFilePath;

    public SecureEnvFileBuilder(Map<String, String> secrets) throws IOException {
        Path tmpDir = Path.of(System.getProperty("java.io.tmpdir"));
        boolean isPosix = Files.getFileStore(tmpDir).supportsFileAttributeView("posix");

        if (isPosix) {
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
            this.envFilePath = Files.createTempFile(PREFIX, SUFFIX, PosixFilePermissions.asFileAttribute(perms));
        } else {
            // Windows fallback: Best-effort, does not guarantee 0600 POSIX equivalence.
            this.envFilePath = Files.createTempFile(PREFIX, SUFFIX);
            this.envFilePath.toFile().setReadable(false, false);
            this.envFilePath.toFile().setWritable(false, false);
            this.envFilePath.toFile().setReadable(true, true);
            this.envFilePath.toFile().setWritable(true, true);
        }

        StringBuilder content = new StringBuilder();
        if (secrets != null) {
            for (Map.Entry<String, String> entry : secrets.entrySet()) {
                content.append(entry.getKey()).append("=").append(entry.getValue()).append("\n");
            }
        }
        Files.writeString(this.envFilePath, content.toString());
    }

    public Path getEnvFilePath() {
        return envFilePath;
    }

    @Override
    public void close() {
        try {
            Files.deleteIfExists(this.envFilePath);
        } catch (IOException e) {
            logger.warn("Failed to delete secure env file: {}", envFilePath, e);
        }
    }

    public static void cleanupOrphans() {
        Path tmpDir = Path.of(System.getProperty("java.io.tmpdir"));
        Instant threshold = Instant.now().minus(30, ChronoUnit.MINUTES);
        
        try (var stream = Files.newDirectoryStream(tmpDir, PREFIX + "*" + SUFFIX)) {
            for (Path path : stream) {
                try {
                    FileTime lastModified = Files.getLastModifiedTime(path);
                    if (lastModified.toInstant().isBefore(threshold)) {
                        Files.deleteIfExists(path);
                        logger.info("Purged orphaned stale secret env file: {}", path.getFileName());
                    }
                } catch (IOException e) {
                    logger.warn("Failed to inspect/purge orphaned secure env file: {}", path.getFileName());
                }
            }
        } catch (IOException e) {
            logger.warn("Error scanning for orphaned secure env files", e);
        }
    }
}