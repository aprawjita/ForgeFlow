package forgeflow_api.workflow;

import forgeflow_api.repository.ConnectedRepository;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface WorkflowDao extends JpaRepository<Workflow, Long> {
    List<Workflow> findByConnectedRepository(ConnectedRepository repository);
}