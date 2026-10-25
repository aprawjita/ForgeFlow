package forgeflow_api.workflow;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WorkflowJobDao extends JpaRepository<WorkflowJob, Long> {
    
    // Restored existing method
    List<WorkflowJob> findByWorkflow(Workflow workflow);

    // Reverse lookup for DAG Traversal: "Find jobs that depend on THIS upstream job"
    List<WorkflowJob> findByDependsOnContaining(WorkflowJob upstreamJob);
}