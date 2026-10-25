package forgeflow_api.workflow.secrets;

import forgeflow_api.vault.ForgeFlowVaultException;
import forgeflow_api.vault.VaultSecretProvider;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class WorkflowSecretProvider {
    private static final String MOUNT = "secret";
    private static final String PATH_PREFIX = "forgeflow/repositories/";

    private final VaultSecretProvider vaultProvider;

    public WorkflowSecretProvider(VaultSecretProvider vaultProvider) {
        this.vaultProvider = vaultProvider;
    }

    /**
     * Resolves references to actual Vault values securely.
     * @param repositoryId The repository context (enforces structural isolation).
     * @param secretReferences Map of <EnvironmentVariableName, VaultSecretName>
     * @return Map of <EnvironmentVariableName, ActualSecretValue>
     */
    public Map<String, String> resolveSecrets(Long repositoryId, Map<String, String> secretReferences) {
        if (secretReferences == null || secretReferences.isEmpty()) {
            return Map.of();
        }

        Map<String, String> resolvedSecrets = new HashMap<>();
        for (Map.Entry<String, String> entry : secretReferences.entrySet()) {
            String envVarName = entry.getKey();
            String secretName = entry.getValue();
            
            String path = PATH_PREFIX + repositoryId + "/secrets/" + secretName;
            try {
                String value = vaultProvider.readSecretValue(MOUNT, path, "value");
                resolvedSecrets.put(envVarName, value);
            } catch (ForgeFlowVaultException e) {
                // FAIL-CLOSED: Missing secret or Vault outage halts workflow execution entirely.
                throw new IllegalStateException("Workflow execution failed: Required secret '" + secretName + "' is missing or Vault is unavailable.", e);
            }
        }
        return resolvedSecrets;
    }

    /**
     * For backend API provisioning ONLY. Never returns or logs the secret payload.
     */
    public void provisionSecret(Long repositoryId, String secretName, String value) {
        if (value == null || value.isBlank()) return;
        String path = PATH_PREFIX + repositoryId + "/secrets/" + secretName;
        vaultProvider.writeSecret(MOUNT, path, Map.of("value", value));
    }
}