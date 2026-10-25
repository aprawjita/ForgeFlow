package forgeflow_api.vault;

import java.util.Map;

public interface VaultSecretProvider {
    Map<String, Object> readSecret(String mountPath, String logicalPath);
    String readSecretValue(String mountPath, String logicalPath, String key);
    void writeSecret(String mountPath, String logicalPath, Map<String, Object> data);
}