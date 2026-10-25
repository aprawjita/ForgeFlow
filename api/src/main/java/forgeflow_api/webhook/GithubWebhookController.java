package forgeflow_api.webhook;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/webhooks")
@RequiredArgsConstructor
public class GithubWebhookController {

    private static final Logger logger = LoggerFactory.getLogger(GithubWebhookController.class);
    private final WebhookSignatureValidator signatureValidator;
    private final GithubWebhookService webhookService;
    private final ObjectMapper objectMapper;

    @PostMapping("/github")
    public ResponseEntity<String> handleGithubWebhook(
            @RequestHeader(value = "X-Hub-Signature-256", required = false) String signature,
            @RequestHeader(value = "X-GitHub-Event", required = false) String eventType,
            @RequestBody String rawPayload
    ) {
        // 1. Verify Cryptographic Signature
        if (!signatureValidator.isValidSignature(rawPayload, signature)) {
            logger.warn("Rejected webhook: Invalid HMAC Signature");
            return ResponseEntity.status(401).body("Invalid HMAC Signature");
        }

        // 2. Only process push events for now
        if (!"push".equals(eventType)) {
            return ResponseEntity.ok("Event ignored");
        }

        // 3. Parse JSON and hand off to the service
        try {
            GithubPushPayload payload = objectMapper.readValue(rawPayload, GithubPushPayload.class);
            webhookService.processPushEvent(payload);
        } catch (Exception e) {
            logger.error("Failed to parse GitHub webhook payload", e);
            return ResponseEntity.status(400).body("Invalid payload format");
        }

        // 4. Acknowledge receipt instantly to prevent GitHub timeout
        return ResponseEntity.ok("Webhook processed successfully");
    }
}