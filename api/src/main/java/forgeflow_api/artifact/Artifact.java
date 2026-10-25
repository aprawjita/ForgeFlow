package forgeflow_api.artifact;

import forgeflow_api.workflow.JobRun;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "artifacts")
public class Artifact {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "producing_job_run_id", nullable = false)
    private JobRun producingJobRun;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false, unique = true)
    private String storageKey;

    @Column(nullable = false)
    private Long sizeBytes;

    @CreationTimestamp
    @Column(updatable = false)
    private Instant createdAt;
}