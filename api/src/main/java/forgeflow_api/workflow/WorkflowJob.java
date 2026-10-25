package forgeflow_api.workflow;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.List;
import java.util.Set;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "workflow_jobs")
public class WorkflowJob {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "workflow_id", nullable = false)
    private Workflow workflow;

    @Column(nullable = false)
    private String name;

    @ManyToMany(fetch = FetchType.LAZY)
    @JoinTable(
            name = "workflow_job_dependencies",
            joinColumns = @JoinColumn(name = "job_id"),
            inverseJoinColumns = @JoinColumn(name = "depends_on_job_id")
    )
    private Set<WorkflowJob> dependsOn;

    @OneToMany(mappedBy = "workflowJob", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<WorkflowStep> steps;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "workflow_job_produces", joinColumns = @JoinColumn(name = "job_id"))
    @Column(name = "artifact_path")
    private List<String> produces;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "workflow_job_consumes", joinColumns = @JoinColumn(name = "job_id"))
    @Column(name = "artifact_name")
    private List<String> consumes;

    @CreationTimestamp
    @Column(updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    private Instant updatedAt;

    // --- VAULT SECRET REFERENCES ---
    @Builder.Default
    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "workflow_job_secrets", joinColumns = @JoinColumn(name = "job_id"))
    @MapKeyColumn(name = "env_var_name")
    @Column(name = "vault_secret_name")
    private java.util.Map<String, String> secrets = new java.util.HashMap<>();
}