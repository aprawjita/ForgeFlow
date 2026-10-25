package forgeflow_api.repository;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ConnectedRepositoryResponse {
    private Long id;
    private Long githubRepoId;
    private String name;
    private String fullName;
    private String githubUrl;
    private String cloneUrl;
    private String defaultBranch;
    private boolean isPrivate;
    private String ownerEmail;
    private Instant createdAt;
}