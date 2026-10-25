package forgeflow_api.workflow;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WorkflowRunDao extends JpaRepository<WorkflowRun, Long> {
}