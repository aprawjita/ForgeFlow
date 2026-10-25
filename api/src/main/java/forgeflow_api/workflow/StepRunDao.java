package forgeflow_api.workflow;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface StepRunDao extends JpaRepository<StepRun, Long> {
    List<StepRun> findByJobRun(JobRun jobRun);
}