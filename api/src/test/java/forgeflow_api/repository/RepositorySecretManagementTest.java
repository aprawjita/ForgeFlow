package forgeflow_api.repository;

import forgeflow_api.vault.ForgeFlowVaultException;
import forgeflow_api.vault.VaultSecretProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RepositorySecretManagementTest {

    @Mock
    private VaultSecretProvider vaultSecretProvider;

    private RepositoryCredentialProvider credentialProvider;

    @BeforeEach
    void setUp() {
        // Instantiate the provider directly with the mock, avoiding Spring Context completely
        credentialProvider = new RepositoryCredentialProvider(vaultSecretProvider);
    }

    @Test
    void testCanonicalUrlPersistence() {
        // Verify canonical URLs successfully pass validation
        assertDoesNotThrow(() -> RepositoryUrlValidator.validateCanonicalUrl("https://github.com/org/repo.git"));
    }

    @Test
    void testCredentialBearingUrlRejection() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            RepositoryUrlValidator.validateCanonicalUrl("https://user:secret-token-123@github.com/org/repo.git");
        });
        
        // Assert the validation actively rejects credentials
        assertTrue(ex.getMessage().contains("must not contain embedded credentials"));
        // Assert the exception absolutely prevents token leakage in standard out/logs
        assertFalse(ex.getMessage().contains("secret-token-123")); 
    }

    @Test
    void testVaultCredentialMissing() {
        Long testRepoId = 999L;
        
        // Mock Vault failing to find the secret
        when(vaultSecretProvider.readSecretValue("secret", "forgeflow/repositories/999/credentials", "token"))
                .thenThrow(new ForgeFlowVaultException("Not found"));
        
        // Missing credential strictly fails closed (returns empty)
        assertTrue(credentialProvider.getCredential(testRepoId).isEmpty());
    }

    @Test
    void testVaultCredentialRetrievalAndStore() {
        Long testRepoId = 999L;

        // Mock Vault returning the secret successfully
        when(vaultSecretProvider.readSecretValue("secret", "forgeflow/repositories/999/credentials", "token"))
                .thenReturn("ghp_test_token_xyz");
                
        // Assert the abstraction returns it
        assertEquals("ghp_test_token_xyz", credentialProvider.getCredential(testRepoId).get());

        // Verify the store method writes exactly the right map structure to the right Vault path
        credentialProvider.storeCredential(testRepoId, "ghp_test_token_xyz");
        verify(vaultSecretProvider).writeSecret("secret", "forgeflow/repositories/999/credentials", Map.of("token", "ghp_test_token_xyz"));
    }
}