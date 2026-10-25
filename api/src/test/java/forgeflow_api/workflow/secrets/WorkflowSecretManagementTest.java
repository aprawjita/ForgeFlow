package forgeflow_api.workflow.secrets;

import forgeflow_api.vault.ForgeFlowVaultException;
import forgeflow_api.vault.VaultSecretProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class WorkflowSecretManagementTest {

    @Mock
    private VaultSecretProvider vaultSecretProvider;

    private WorkflowSecretProvider secretProvider;

    @BeforeEach
    void setUp() {
        secretProvider = new WorkflowSecretProvider(vaultSecretProvider);
    }

    @Test
    void testSecretResolutionSuccess() {
        Long repoId = 42L;
        when(vaultSecretProvider.readSecretValue("secret", "forgeflow/repositories/42/secrets/PROD_DB_PASSWORD", "value"))
                .thenReturn("super_secret_db_pass");

        Map<String, String> references = Map.of("DB_PASS", "PROD_DB_PASSWORD");
        Map<String, String> resolved = secretProvider.resolveSecrets(repoId, references);
        
        assertEquals(1, resolved.size());
        assertEquals("super_secret_db_pass", resolved.get("DB_PASS"));
    }

    @Test
    void testMissingSecretFailsClosed() {
        Long repoId = 42L;
        when(vaultSecretProvider.readSecretValue("secret", "forgeflow/repositories/42/secrets/MISSING_SECRET", "value"))
                .thenThrow(new ForgeFlowVaultException("Not found"));

        Map<String, String> references = Map.of("API_KEY", "MISSING_SECRET");
        
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> {
            secretProvider.resolveSecrets(repoId, references);
        });
        
        assertTrue(ex.getMessage().contains("Required secret 'MISSING_SECRET' is missing"));
    }

    @Test
    void testSecureEnvFileBuilderLifecycle() throws Exception {
        Map<String, String> resolvedSecrets = Map.of("API_KEY", "test_vault_value");
        
        String tempFilePath;
        try (SecureEnvFileBuilder builder = new SecureEnvFileBuilder(resolvedSecrets)) {
            tempFilePath = builder.getEnvFilePath().toString();
            assertTrue(Files.exists(builder.getEnvFilePath()));
            
            String content = Files.readString(builder.getEnvFilePath());
            assertTrue(content.contains("API_KEY=test_vault_value"));
        }
        
        // Assert automatic cleanup destroys the secret payload
        assertFalse(Files.exists(Path.of(tempFilePath)));
    }

    @Test
    void testRepositoryIsolationEnforced() {
        secretProvider.provisionSecret(999L, "MY_SECRET", "val");
        // Physically verifies the abstraction scopes the path to the provided repo ID, preventing cross-tenant injection
        verify(vaultSecretProvider).writeSecret("secret", "forgeflow/repositories/999/secrets/MY_SECRET", Map.of("value", "val"));
    }
}