package forgeflow_api.workflow.secrets;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SecureEnvFileBuilderTest {

    @Test
    void testEnvFileLocationAndNameSafe() throws IOException {
        Path persistentWorkspace = Paths.get("/tmp/forgeflow-workspaces");
        String secretValue = "SUPER_SECRET_123987";
        
        try (SecureEnvFileBuilder builder = new SecureEnvFileBuilder(Map.of("TEST_KEY", secretValue))) {
            Path filePath = builder.getEnvFilePath();
            
            assertFalse(filePath.toAbsolutePath().startsWith(persistentWorkspace.toAbsolutePath()), 
                "Temp file must be strictly outside the persistent workspace directory");
            
            assertTrue(filePath.getFileName().toString().startsWith("ff-env-secret-"));
            
            assertFalse(filePath.toString().contains(secretValue), 
                "File path MUST NOT contain secret values");
        }
    }

    @Test
    void testPermissionsOnPosix() throws IOException {
        try (SecureEnvFileBuilder builder = new SecureEnvFileBuilder(Map.of("A", "B"))) {
            Path filePath = builder.getEnvFilePath();
            if (Files.getFileStore(filePath).supportsFileAttributeView("posix")) {
                Set<java.nio.file.attribute.PosixFilePermission> perms = Files.getPosixFilePermissions(filePath);
                assertEquals(PosixFilePermissions.fromString("rw-------"), perms, "File permissions must be explicitly 0600 on POSIX");
            }
        }
    }

    @Test
    void testNormalTryWithResourcesCleanup() throws IOException {
        Path capturedPath;
        try (SecureEnvFileBuilder builder = new SecureEnvFileBuilder(Map.of("A", "B"))) {
            capturedPath = builder.getEnvFilePath();
            assertTrue(Files.exists(capturedPath));
        }
        assertFalse(Files.exists(capturedPath), "Env file must be deleted after try-with-resources block");
    }

    @Test
    void testExceptionBlockCleanup() throws IOException {
        Path capturedPath = null;
        try {
            try (SecureEnvFileBuilder builder = new SecureEnvFileBuilder(Map.of("A", "B"))) {
                capturedPath = builder.getEnvFilePath();
                throw new RuntimeException("Simulated execution failure");
            }
        } catch (RuntimeException ignored) {
        }
        assertNotNull(capturedPath);
        assertFalse(Files.exists(capturedPath), "Env file must be deleted even if execution throws an exception");
    }

    @Test
    void testConcurrentCreationIsolation() throws IOException {
        try (SecureEnvFileBuilder b1 = new SecureEnvFileBuilder(Map.of("K1", "V1"));
             SecureEnvFileBuilder b2 = new SecureEnvFileBuilder(Map.of("K2", "V2"))) {
            assertNotEquals(b1.getEnvFilePath(), b2.getEnvFilePath(), "Concurrent files must have distinct paths");
            assertTrue(Files.exists(b1.getEnvFilePath()));
            assertTrue(Files.exists(b2.getEnvFilePath()));
        }
    }

    @Test
    void testOrphanSweeperRespectsAgeThreshold() throws IOException {
        Path staleFile = Files.createTempFile("ff-env-secret-", ".env");
        Files.setLastModifiedTime(staleFile, FileTime.from(Instant.now().minus(60, ChronoUnit.MINUTES)));
        
        Path activeFile = Files.createTempFile("ff-env-secret-", ".env");
        
        SecureEnvFileBuilder.cleanupOrphans();
        
        assertFalse(Files.exists(staleFile), "Stale orphan must be deleted");
        assertTrue(Files.exists(activeFile), "Active execution file must NOT be deleted by sweeper");
        
        Files.deleteIfExists(activeFile); // Cleanup test artifact
    }
}