package forgeflow_api.vault;

import org.springframework.context.annotation.Configuration;

@Configuration
public class VaultConfig {
    // Purposely empty. 
    // Vault initialization is now safely isolated inside SpringVaultSecretProvider 
    // to prevent Spring ApplicationContext crashes during tests.
}