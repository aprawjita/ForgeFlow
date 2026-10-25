package forgeflow_api.workflow;

import forgeflow_api.repository.ConnectedRepository;
import forgeflow_api.repository.ConnectedRepositoryDao;
import forgeflow_api.user.User;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class WorkflowService {

    private final WorkflowDao workflowDao;
    private final ConnectedRepositoryDao connectedRepositoryDao;

    @Transactional
    public WorkflowResponse createWorkflow(Long repositoryId, WorkflowRequest request, User user) {
        ConnectedRepository repository = connectedRepositoryDao.findById(repositoryId)
                .orElseThrow(() -> new RuntimeException("Repository not found"));

        Workflow workflow = Workflow.builder()
                .connectedRepository(repository)
                .name(request.getName())
                .configuration(request.getConfiguration())
                .enabled(request.isEnabled())
                .build();

        List<WorkflowJob> jobs = buildJobsFromRequest(request.getJobs(), workflow);
        
        validateArtifactDependencies(jobs);
        
        workflow.setJobs(jobs);
        workflow = workflowDao.save(workflow);

        return mapToResponse(workflow);
    }

    @Transactional(readOnly = true)
    public List<WorkflowResponse> getWorkflowsForRepository(Long repositoryId, User user) {
        return workflowDao.findAll().stream()
                .filter(w -> w.getConnectedRepository().getId().equals(repositoryId))
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public WorkflowResponse getWorkflowById(Long workflowId, User user) {
        Workflow workflow = workflowDao.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("Workflow not found"));
        return mapToResponse(workflow);
    }

    @Transactional
    public WorkflowResponse updateWorkflow(Long workflowId, WorkflowRequest request, User user) {
        Workflow workflow = workflowDao.findById(workflowId)
                .orElseThrow(() -> new RuntimeException("Workflow not found"));

        workflow.setName(request.getName());
        workflow.setConfiguration(request.getConfiguration());
        workflow.setEnabled(request.isEnabled());

        workflow.getJobs().clear();
        List<WorkflowJob> newJobs = buildJobsFromRequest(request.getJobs(), workflow);
        
        validateArtifactDependencies(newJobs);
        
        workflow.getJobs().addAll(newJobs);
        workflow = workflowDao.save(workflow);

        return mapToResponse(workflow);
    }

    @Transactional
    public void deleteWorkflow(Long workflowId, User user) {
        workflowDao.deleteById(workflowId);
    }

    private List<WorkflowJob> buildJobsFromRequest(List<WorkflowJobDto> jobDtos, Workflow workflow) {
        if (jobDtos == null || jobDtos.isEmpty()) return new ArrayList<>();

        Map<String, WorkflowJob> jobMap = new HashMap<>();
        for (WorkflowJobDto dto : jobDtos) {
            WorkflowJob job = mapToEntity(dto, workflow);
            jobMap.put(job.getName(), job);
        }

        for (WorkflowJobDto dto : jobDtos) {
            WorkflowJob currentJob = jobMap.get(dto.getName());
            Set<WorkflowJob> dependencies = new HashSet<>();
            if (dto.getDependsOnJobNames() != null) {
                for (String depName : dto.getDependsOnJobNames()) {
                    WorkflowJob depJob = jobMap.get(depName);
                    if (depJob != null) {
                        dependencies.add(depJob);
                    }
                }
            }
            currentJob.setDependsOn(dependencies);
        }

        return new ArrayList<>(jobMap.values());
    }

    private WorkflowJob mapToEntity(WorkflowJobDto dto, Workflow workflow) {
        WorkflowJob job = WorkflowJob.builder()
                .workflow(workflow)
                .name(dto.getName())
                .produces(dto.getProduces() != null ? new ArrayList<>(dto.getProduces()) : new ArrayList<>())
                .consumes(dto.getConsumes() != null ? new ArrayList<>(dto.getConsumes()) : new ArrayList<>())
                .build();

        if (dto.getSteps() != null) {
            List<WorkflowStep> steps = new ArrayList<>();
            for (WorkflowStepDto stepDto : dto.getSteps()) {
                WorkflowStep step = WorkflowStep.builder()
                        .name(stepDto.getName())
                        .command(stepDto.getCommand())
                        .executionOrder(stepDto.getExecutionOrder())
                        .build();
                step.setWorkflowJob(job);
                steps.add(step);
            }
            job.setSteps(steps);
        } else {
            job.setSteps(new ArrayList<>());
        }

        return job;
    }

    public void validateArtifactDependencies(List<WorkflowJob> jobs) {
        for (WorkflowJob job : jobs) {
            if (job.getConsumes() == null || job.getConsumes().isEmpty()) {
                continue;
            }

            for (String consumedArtifact : job.getConsumes()) {
                boolean producerFound = hasUpstreamProducer(job, consumedArtifact, new HashSet<>());
                if (!producerFound) {
                    throw new IllegalArgumentException(
                        String.format("Job '%s' consumes artifact '%s', but no upstream job in the workflow produces it.", 
                        job.getName(), consumedArtifact)
                    );
                }
            }
        }
    }

    private boolean hasUpstreamProducer(WorkflowJob job, String artifactName, Set<WorkflowJob> visited) {
        if (job.getDependsOn() == null || job.getDependsOn().isEmpty()) {
            return false;
        }

        for (WorkflowJob parent : job.getDependsOn()) {
            if (visited.contains(parent)) continue; 
            visited.add(parent);

            if (parent.getProduces() != null && parent.getProduces().contains(artifactName)) {
                return true;
            }

            if (hasUpstreamProducer(parent, artifactName, visited)) {
                return true;
            }
        }
        return false;
    }

    private WorkflowResponse mapToResponse(Workflow workflow) {
        return WorkflowResponse.builder()
                .id(workflow.getId())
                .name(workflow.getName())
                .configuration(workflow.getConfiguration())
                .enabled(workflow.isEnabled())
                .createdAt(workflow.getCreatedAt())
                .updatedAt(workflow.getUpdatedAt())
                .build();
    }
}