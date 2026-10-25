package forgeflow_api.vault;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Primary
@Component
@ConditionalOnProperty(prefix = "forgeflow.vault", name = "enabled", havingValue = "false", matchIfMissing = true)
public class MockVaultSecretProvider implements VaultSecretProvider {

    private static final String FAKE_KEY = "c3VwZXItc2VjcmV0LXZhdWx0LWp3dC1rZXktMjU2LWJpdHMtbG9uZy0xMjM0NTY3ODkw";
    private final Map<String, Map<String, Object>> storage = new ConcurrentHashMap<>();

    @Override
    public void writeSecret(String mountPath, String logicalPath, Map<String, Object> data) {
        storage.put(mountPath + "/" + logicalPath, data);
    }

    @Override
    public Map<String, Object> readSecret(String mountPath, String logicalPath) {
        if ("secret/forgeflow/jwt".equals(mountPath + "/" + logicalPath)) {
            return Map.of("secret-key", FAKE_KEY);
        }
        Map<String, Object> result = storage.get(mountPath + "/" + logicalPath);
        if (result == null) {
            throw new ForgeFlowVaultException("Mock not configured for path: " + logicalPath);
        }
        return result;
    }

    @Override
    public String readSecretValue(String mountPath, String logicalPath, String key) {
        Map<String, Object> data = readSecret(mountPath, logicalPath);
        Object value = data.get(key);
        if (value == null) {
            throw new ForgeFlowVaultException("Key '" + key + "' not found in mock path");
        }
        return value.toString();
    }
}