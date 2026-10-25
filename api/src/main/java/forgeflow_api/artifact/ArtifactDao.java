package forgeflow_api.artifact;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ArtifactDao extends JpaRepository<Artifact, Long> {
    List<Artifact> findByProducingJobRunId(Long jobRunId);

    @Query("SELECT a FROM Artifact a WHERE a.producingJobRun.workflowRun.id = :workflowRunId AND a.name = :name")
    List<Artifact> findByWorkflowRunIdAndName(@Param("workflowRunId") Long workflowRunId, @Param("name") String name);
}