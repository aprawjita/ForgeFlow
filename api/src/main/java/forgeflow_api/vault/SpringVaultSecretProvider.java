package forgeflow_api.vault;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.vault.VaultException;
import org.springframework.vault.authentication.AppRoleAuthentication;
import org.springframework.vault.authentication.AppRoleAuthenticationOptions;
import org.springframework.vault.authentication.ClientAuthentication;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultClients;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.core.VaultVersionedKeyValueOperations;
import org.springframework.vault.support.Versioned;

import java.net.URI;
import java.util.Map;

@Slf4j
@Service
public class SpringVaultSecretProvider implements VaultSecretProvider {

    private final VaultProperties properties;
    private final VaultTemplate vaultTemplate;

    public SpringVaultSecretProvider(VaultProperties properties) {
        this.properties = properties;
        if (properties.isEnabled() && properties.getAddress() != null) {
            this.vaultTemplate = createVaultTemplate(properties);
        } else {
            this.vaultTemplate = null;
        }
    }

    private VaultTemplate createVaultTemplate(VaultProperties props) {
        VaultEndpoint endpoint = VaultEndpoint.from(URI.create(props.getAddress()));
        ClientAuthentication clientAuth;

        if (props.getAuthMode() == VaultProperties.AuthMode.APPROLE) {
            AppRoleAuthenticationOptions options = AppRoleAuthenticationOptions.builder()
                    .roleId(AppRoleAuthenticationOptions.RoleId.provided(props.getRoleId() != null ? props.getRoleId() : ""))
                    .secretId(AppRoleAuthenticationOptions.SecretId.provided(props.getSecretId() != null ? props.getSecretId() : ""))
                    .path(props.getApprolePath() != null ? props.getApprolePath() : "approle")
                    .build();
            clientAuth = new AppRoleAuthentication(options, VaultClients.createRestTemplate(endpoint, null));
        } else {
            clientAuth = new TokenAuthentication(props.getToken() != null ? props.getToken() : "");
        }
        return new VaultTemplate(endpoint, clientAuth);
    }

    @Override
    public Map<String, Object> readSecret(String mountPath, String logicalPath) {
        if (!properties.isEnabled() || vaultTemplate == null) {
            throw new ForgeFlowVaultException("Vault integration is disabled");
        }
        
        try {
            VaultVersionedKeyValueOperations ops = vaultTemplate.opsForVersionedKeyValue(mountPath);
            Versioned<Map<String, Object>> response = ops.get(logicalPath);

            if (response == null || !response.hasData()) {
                throw new ForgeFlowVaultException("Secret not found at path: " + mountPath + "/" + logicalPath);
            }
            return response.getData();
            
        } catch (ForgeFlowVaultException e) {
            throw e; 
        } catch (VaultException e) {
            log.error("Vault access failed or denied for path {}/{}", mountPath, logicalPath);
            throw new ForgeFlowVaultException("Failed to read secret from Vault: Server rejected request or is unreachable.");
        } catch (Exception e) {
            log.error("Unexpected internal error accessing Vault path {}/{}", mountPath, logicalPath);
            throw new ForgeFlowVaultException("Failed to read secret from Vault due to an internal error.");
        }
    }

    @Override
    public String readSecretValue(String mountPath, String logicalPath, String key) {
        Map<String, Object> data = readSecret(mountPath, logicalPath);
        Object value = data.get(key);
        if (value == null) {
            throw new ForgeFlowVaultException("Key '" + key + "' not found in secret path: " + mountPath + "/" + logicalPath);
        }
        return value.toString();
    }

    @Override
    public void writeSecret(String mountPath, String logicalPath, Map<String, Object> data) {
        if (!properties.isEnabled() || vaultTemplate == null) {
            throw new ForgeFlowVaultException("Vault integration is disabled");
        }
        try {
            VaultVersionedKeyValueOperations ops = vaultTemplate.opsForVersionedKeyValue(mountPath);
            ops.put(logicalPath, data);
        } catch (Exception e) {
            log.error("Unexpected internal error writing to Vault path {}/{}", mountPath, logicalPath);
            throw new ForgeFlowVaultException("Failed to write secret to Vault due to an internal error.");
        }
    }
}