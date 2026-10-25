package forgeflow_api.repository;

import forgeflow_api.user.User;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ConnectedRepositoryService {

    private final ConnectedRepositoryDao repositoryDao;

    public ConnectedRepositoryResponse connectRepository(ConnectRepositoryRequest request, User currentUser) {
        // ENFORCE SECURITY: Reject any URL that contains an embedded password or token
        RepositoryUrlValidator.validateCanonicalUrl(request.getCloneUrl());

        if (repositoryDao.existsByGithubRepoIdAndOwner(request.getGithubRepoId(), currentUser)) {
            throw new IllegalArgumentException("Repository is already connected to this account");
        }

        var repo = ConnectedRepository.builder()
                .owner(currentUser)
                .githubRepoId(request.getGithubRepoId())
                .name(request.getName())
                .fullName(request.getFullName())
                .githubUrl(request.getGithubUrl())
                .cloneUrl(request.getCloneUrl())
                .defaultBranch(request.getDefaultBranch())
                .isPrivate(request.isPrivate())
                .build();

        var savedRepo = repositoryDao.save(repo);
        return mapToResponse(savedRepo);
    }

    public List<ConnectedRepositoryResponse> getUserRepositories(User currentUser) {
        return repositoryDao.findByOwner(currentUser)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    public ConnectedRepository getValidatedRepository(Long repoId, User currentUser) {
        return repositoryDao.findByIdAndOwner(repoId, currentUser)
                .orElseThrow(() -> new IllegalArgumentException("Connected repository not found or unauthorized"));
    }

    private ConnectedRepositoryResponse mapToResponse(ConnectedRepository repo) {
        return ConnectedRepositoryResponse.builder()
                .id(repo.getId())
                .githubRepoId(repo.getGithubRepoId())
                .name(repo.getName())
                .fullName(repo.getFullName())
                .githubUrl(repo.getGithubUrl())
                .cloneUrl(repo.getCloneUrl())
                .defaultBranch(repo.getDefaultBranch())
                .isPrivate(repo.isPrivate())
                .ownerEmail(repo.getOwner().getEmail())
                .createdAt(repo.getCreatedAt())
                .build();
    }
}