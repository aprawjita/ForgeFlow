package forgeflow_api.workflow;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface JobRunDao extends JpaRepository<JobRun, Long> {
    List<JobRun> findByWorkflowRun(WorkflowRun workflowRun);
    Optional<JobRun> findByWorkflowRunAndWorkflowJob(WorkflowRun workflowRun, WorkflowJob workflowJob);
}