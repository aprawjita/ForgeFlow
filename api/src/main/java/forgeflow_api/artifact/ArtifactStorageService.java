package forgeflow_api.artifact;

import forgeflow_api.workflow.JobRun;
import forgeflow_api.workflow.WorkflowRun;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;
import java.nio.file.*;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

@Service
public class ArtifactStorageService {

    private final S3Client s3Client;
    private final String bucket;
    private final ArtifactDao artifactDao;

    public ArtifactStorageService(S3Client s3Client, 
                                  @Value("${forgeflow.storage.bucket}") String bucket,
                                  ArtifactDao artifactDao) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.artifactDao = artifactDao;
    }

    public void uploadArtifact(JobRun jobRun, String artifactPath, Path workspaceRoot) {
        try {
            Path targetPath = workspaceRoot.resolve(artifactPath).normalize();
            if (!targetPath.startsWith(workspaceRoot)) {
                throw new SecurityException("Path traversal attempt detected: " + artifactPath);
            }
            if (!Files.exists(targetPath)) {
                throw new FileNotFoundException("Declared artifact path does not exist: " + artifactPath);
            }

            Path uploadTarget = targetPath;
            boolean isDirectory = Files.isDirectory(targetPath);
            
            if (isDirectory) {
                uploadTarget = Files.createTempFile("artifact-", ".zip");
                zipDirectory(targetPath, uploadTarget);
            }

            String artifactName = Path.of(artifactPath).getFileName().toString();
            String storageKey = "artifacts/workflows/" + jobRun.getWorkflowRun().getId() + 
                                "/jobs/" + jobRun.getId() + "/" + artifactName + (isDirectory ? ".zip" : "");

            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(storageKey)
                    .build();

            s3Client.putObject(request, uploadTarget);

            Artifact artifact = Artifact.builder()
                    .producingJobRun(jobRun)
                    .name(artifactName)
                    .storageKey(storageKey)
                    .sizeBytes(Files.size(uploadTarget))
                    .build();

            artifactDao.save(artifact);

            if (isDirectory) {
                Files.deleteIfExists(uploadTarget);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to upload artifact: " + artifactPath, e);
        }
    }

    public void downloadArtifact(WorkflowRun workflowRun, String artifactName, Path workspaceRoot) {
        try {
            List<Artifact> artifacts = artifactDao.findByWorkflowRunIdAndName(workflowRun.getId(), artifactName);

            if (artifacts.isEmpty()) {
                throw new FileNotFoundException("Required artifact not found in this workflow run: " + artifactName);
            }

            Artifact artifact = artifacts.get(artifacts.size() - 1); 
            
            Path downloadTemp = Files.createTempFile("download-", artifactName);
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(artifact.getStorageKey())
                    .build();
            
            s3Client.getObject(getRequest, downloadTemp);

            Path destinationDir = workspaceRoot.resolve("artifacts").resolve(artifactName).normalize();
            Files.createDirectories(destinationDir);

            if (artifact.getStorageKey().endsWith(".zip")) {
                unzipFile(downloadTemp, destinationDir, workspaceRoot);
            } else {
                Files.copy(downloadTemp, destinationDir.resolve(artifactName), StandardCopyOption.REPLACE_EXISTING);
            }
            
            Files.deleteIfExists(downloadTemp);

        } catch (Exception e) {
            throw new RuntimeException("Failed to download artifact: " + artifactName, e);
        }
    }

    private void zipDirectory(Path sourceDir, Path zipFile) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile.toFile()))) {
            Files.walk(sourceDir).filter(path -> !Files.isDirectory(path)).forEach(path -> {
                ZipEntry zipEntry = new ZipEntry(sourceDir.relativize(path).toString());
                try {
                    zos.putNextEntry(zipEntry);
                    Files.copy(path, zos);
                    zos.closeEntry();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void unzipFile(Path zipFile, Path destDir, Path workspaceRoot) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile.toFile()))) {
            ZipEntry zipEntry = zis.getNextEntry();
            while (zipEntry != null) {
                Path newPath = destDir.resolve(zipEntry.getName()).normalize();
                if (!newPath.startsWith(workspaceRoot)) {
                    throw new SecurityException("ZIP slip vulnerability detected: " + zipEntry.getName());
                }
                if (zipEntry.isDirectory()) {
                    Files.createDirectories(newPath);
                } else {
                    Files.createDirectories(newPath.getParent());
                    Files.copy(zis, newPath, StandardCopyOption.REPLACE_EXISTING);
                }
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
        }
    }
}