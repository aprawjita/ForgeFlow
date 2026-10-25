package forgeflow_api.repository;

import forgeflow_api.user.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ConnectedRepositoryDao extends JpaRepository<ConnectedRepository, Long> {
    
    List<ConnectedRepository> findByOwner(User owner);
    List<ConnectedRepository> findByGithubRepoId(Long githubRepoId);
    
    boolean existsByGithubRepoIdAndOwner(Long githubRepoId, User owner);
    
    Optional<ConnectedRepository> findByIdAndOwner(Long id, User owner);
}