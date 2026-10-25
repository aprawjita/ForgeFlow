package forgeflow_api.workflow; // Ensure this matches your actual package structure!

import forgeflow_api.repository.ConnectedRepository;
import forgeflow_api.repository.RepositoryCredentialProvider;
import forgeflow_api.worker.logs.LogMaskingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RepositoryCheckoutService {

    private final RepositoryCredentialProvider credentialProvider;
    private final LogMaskingService logMaskingService;

    public void checkoutRepository(ConnectedRepository repo, String branch, Path workspaceDir) {
        log.info("Cloning repository {} (branch: {}) into workspace {}", repo.getFullName(), branch, workspaceDir);

        // 1. Programmatically Sanitize URL (Ensure absolutely no credentials exist in the CLI argument)
        String safeCloneUrl = sanitizeUrl(repo.getCloneUrl());

        ProcessBuilder pb = new ProcessBuilder(
                "git", "clone", "--depth", "1", "--branch", branch, safeCloneUrl, workspaceDir.toString()
        );

        String token = null;
        Path askpassFile = null;

        try {
            if (repo.isPrivate()) {
                token = credentialProvider.getCredential(repo.getId())
                        .orElseThrow(() -> new IllegalStateException("Missing required Vault repository credential for private checkout."));

                // 2. Create Ephemeral AskPass Script (Isolated per execution)
                askpassFile = createEphemeralAskpassScript();

                // 3. Secure Ephemeral Injection (Values exist only in memory, never in CLI arguments)
                pb.environment().put("FORGEFLOW_REPO_TOKEN", token);
                pb.environment().put("GIT_ASKPASS", askpassFile.toAbsolutePath().toString());
                pb.environment().put("GIT_TERMINAL_PROMPT", "0");
            }

            Process process = pb.start();
            boolean finished = process.waitFor(5, TimeUnit.MINUTES);

            if (!finished || process.exitValue() != 0) {
                String errorOutput = new String(process.getErrorStream().readAllBytes());

                // MASKING BOUNDARY: Scrub Git errors before logging
                if (token != null) {
                    errorOutput = logMaskingService.mask(errorOutput, List.of(token));
                }

                log.error("Git checkout failed: {}", errorOutput);
                throw new RuntimeException("Repository checkout failed.");
            }
        } catch (Exception e) {
            String errorMsg = e.getMessage() != null ? e.getMessage() : "Unknown error";

            // MASKING BOUNDARY: Scrub exception traces before throwing
            if (token != null) {
                errorMsg = logMaskingService.mask(errorMsg, List.of(token));
            }
            throw new RuntimeException("Error executing git clone: " + errorMsg, e);
        } finally {
            // 4. Guaranteed Cleanup Boundary (Replaces deleteOnExit)
            if (askpassFile != null) {
                try {
                    Files.deleteIfExists(askpassFile);
                } catch (IOException e) {
                    log.warn("Failed to delete ephemeral askpass script: {}", askpassFile, e);
                }
            }
        }
    }

    private Path createEphemeralAskpassScript() throws IOException {
        boolean isWindows = System.getProperty("os.name").toLowerCase().contains("win");
        Path scriptPath;
        if (isWindows) {
            scriptPath = Files.createTempFile("ff-askpass-", ".bat");
            Files.writeString(scriptPath, "@echo off\r\necho %FORGEFLOW_REPO_TOKEN%\r\n");
            // Windows fallback obfuscation
            scriptPath.toFile().setReadable(false, false);
            scriptPath.toFile().setWritable(false, false);
            scriptPath.toFile().setReadable(true, true);
        } else {
            // Secure POSIX creation (0700 to allow execution)
            scriptPath = Files.createTempFile("ff-askpass-", ".sh");
            Files.writeString(scriptPath, "#!/bin/sh\necho $FORGEFLOW_REPO_TOKEN\n");
            scriptPath.toFile().setReadable(false, false);
            scriptPath.toFile().setWritable(false, false);
            scriptPath.toFile().setExecutable(false, false);
            scriptPath.toFile().setReadable(true, true);
            scriptPath.toFile().setWritable(true, true);
            scriptPath.toFile().setExecutable(true, true); // Owner execution required by Git
        }
        return scriptPath;
    }

    private String sanitizeUrl(String rawUrl) {
        try {
            URI uri = new URI(rawUrl);
            // Reconstruct URI strictly without the UserInfo (credentials) component
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()).toString();
        } catch (Exception e) {
            // Fallback if parsing fails - just return raw. (Creation boundary should have validated it).
            return rawUrl;
        }
    }
}