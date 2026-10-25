package forgeflow_api.repository;

import forgeflow_api.vault.ForgeFlowVaultException;
import forgeflow_api.vault.VaultSecretProvider;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;

@Service
public class RepositoryCredentialProvider {
    private static final String MOUNT = "secret";
    private static final String PATH_PREFIX = "forgeflow/repositories/";
    
    private final VaultSecretProvider vaultProvider;

    public RepositoryCredentialProvider(VaultSecretProvider vaultProvider) {
        this.vaultProvider = vaultProvider;
    }

    public void storeCredential(Long repositoryId, String token) {
        if (token == null || token.isBlank()) return;
        vaultProvider.writeSecret(MOUNT, PATH_PREFIX + repositoryId + "/credentials", Map.of("token", token));
    }

    public Optional<String> getCredential(Long repositoryId) {
        try {
            return Optional.of(vaultProvider.readSecretValue(MOUNT, PATH_PREFIX + repositoryId + "/credentials", "token"));
        } catch (ForgeFlowVaultException e) {
            return Optional.empty();
        }
    }
}