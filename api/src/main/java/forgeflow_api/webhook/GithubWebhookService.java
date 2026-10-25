package forgeflow_api.webhook;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.repository.ConnectedRepository;
import forgeflow_api.repository.ConnectedRepositoryDao;
import forgeflow_api.workflow.RunStatus;
import forgeflow_api.workflow.Workflow;
import forgeflow_api.workflow.WorkflowDao;
import forgeflow_api.workflow.WorkflowRun;
import forgeflow_api.workflow.WorkflowRunDao;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class GithubWebhookService {

    private static final Logger logger = LoggerFactory.getLogger(GithubWebhookService.class);

    private final ConnectedRepositoryDao repositoryDao;
    private final WorkflowDao workflowDao;
    private final WorkflowRunDao workflowRunDao;
    private final KafkaProducerService kafkaProducerService;

    @Transactional
    public void processPushEvent(GithubPushPayload payload) {
        Long githubRepoId = payload.getRepository().getId();
        String commitSha = payload.getAfter();
        String branch = payload.getRef().replace("refs/heads/", "");

        List<ConnectedRepository> connectedRepos = repositoryDao.findByGithubRepoId(githubRepoId);

        if (connectedRepos.isEmpty()) {
            logger.warn("Webhook received for untracked repository ID: {}", githubRepoId);
            return;
        }

        for (ConnectedRepository repo : connectedRepos) {
            List<Workflow> workflows = workflowDao.findByConnectedRepository(repo)
                    .stream()
                    .filter(Workflow::isEnabled)
                    .toList();

            for (Workflow workflow : workflows) {
                // 1. Persist initial state
                WorkflowRun run = WorkflowRun.builder()
                        .workflow(workflow)
                        .commitSha(commitSha)
                        .branch(branch)
                        .triggerType("GITHUB_PUSH")
                        .status(RunStatus.PENDING)
                        .build();
                
                run = workflowRunDao.save(run);
                
                // 2. Publish to Kafka with explicit failure strategy
                try {
                    kafkaProducerService.publishWorkflowRunTriggered(run.getId());
                } catch (Exception e) {
                    // Failure Strategy: If Kafka is down, mark the run as failed.
                    // We do not silently ignore this or falsely leave it as PENDING/queued.
                    logger.error("Aborting queue process. Marking run {} as FAILURE due to Kafka unavailability.", run.getId());
                    run.setStatus(RunStatus.FAILURE);
                    workflowRunDao.save(run);
                }
            }
        }
    }
}