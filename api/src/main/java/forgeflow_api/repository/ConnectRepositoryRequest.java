package forgeflow_api.repository;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ConnectRepositoryRequest {
    
    @NotNull(message = "GitHub repository ID is required")
    private Long githubRepoId;

    @NotBlank(message = "Repository name is required")
    private String name;

    @NotBlank(message = "Full repository name is required")
    private String fullName;

    @NotBlank(message = "GitHub URL is required")
    private String githubUrl;

    @NotBlank(message = "Clone URL is required")
    private String cloneUrl;

    @NotBlank(message = "Default branch is required")
    private String defaultBranch;

    private boolean isPrivate;
}