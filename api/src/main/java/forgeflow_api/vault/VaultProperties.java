package forgeflow_api.vault;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "forgeflow.vault")
public class VaultProperties {
    private boolean enabled = false;
    private String address = "http://127.0.0.1:8200";
    private AuthMode authMode = AuthMode.TOKEN;
    private String token;
    private String roleId;
    private String secretId;
    private String approlePath = "approle";

    public enum AuthMode {
        TOKEN, APPROLE
    }
}