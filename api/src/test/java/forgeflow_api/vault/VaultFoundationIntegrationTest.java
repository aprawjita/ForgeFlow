package forgeflow_api.vault;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.security.JwtService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.vault.VaultContainer;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Testcontainers
class VaultFoundationIntegrationTest {

    @Container
    static final VaultContainer<?> vault = new VaultContainer<>(DockerImageName.parse("hashicorp/vault:1.15"))
            .withVaultToken("test-root-token")
            // FIXED: Using a valid Base64 encoded string for the JWT test key
            .withSecretInVault("secret/forgeflow/jwt", "secret-key=c3VwZXItc2VjcmV0LXZhdWx0LWp3dC1rZXktMjU2LWJpdHMtbG9uZy0xMjM0NTY3ODkw")
            .withSecretInVault("secret/forgeflow/test-secret", "my-key=super-fake-value");

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static final GenericContainer<?> postgres = new GenericContainer<>("postgres:15-alpine")
            .withExposedPorts(5432)
            .withEnv("POSTGRES_USER", "testuser")
            .withEnv("POSTGRES_PASSWORD", "testpass")
            .withEnv("POSTGRES_DB", "forgeflow_db");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
        registry.add("spring.datasource.url", () -> String.format("jdbc:postgresql://%s:%d/forgeflow_db", postgres.getHost(), postgres.getFirstMappedPort()));
        registry.add("spring.datasource.username", () -> "testuser");
        registry.add("spring.datasource.password", () -> "testpass");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");

        registry.add("forgeflow.vault.enabled", () -> "true");
        registry.add("forgeflow.vault.address", () -> vault.getHttpHostAddress());
        registry.add("forgeflow.vault.auth-mode", () -> "TOKEN");
        registry.add("forgeflow.vault.token", () -> "test-root-token");
    }

    @Autowired
    private VaultSecretProvider secretProvider;

    @Autowired
    private JwtService jwtService;

    @MockitoBean 
    private KafkaProducerService kafkaProducerService;

    @Test
    void test1_and_2_ReadKv2Secret() {
        String val = secretProvider.readSecretValue("secret", "forgeflow/test-secret", "my-key");
        assertEquals("super-fake-value", val);
    }

    @Test
    void test3_MissingSecret_FailsCleanly() {
        ForgeFlowVaultException ex = assertThrows(ForgeFlowVaultException.class, () -> {
            secretProvider.readSecretValue("secret", "forgeflow/missing", "key");
        });
        assertTrue(ex.getMessage().contains("Secret not found"));
    }

    @Test
    void test4_InvalidAuth_MasksExceptions() {
        VaultProperties badProps = new VaultProperties();
        badProps.setEnabled(true);
        badProps.setAddress(vault.getHttpHostAddress());
        badProps.setAuthMode(VaultProperties.AuthMode.TOKEN);
        badProps.setToken("bad-token");
        VaultSecretProvider badProvider = new SpringVaultSecretProvider(badProps);

        ForgeFlowVaultException ex = assertThrows(ForgeFlowVaultException.class, () -> {
            badProvider.readSecretValue("secret", "forgeflow/test-secret", "my-key");
        });
        assertTrue(ex.getMessage().contains("Failed to read secret"));
        assertFalse(ex.getMessage().contains("bad-token")); 
    }

    @Test
    void test5_VaultUnavailable() {
        VaultProperties badProps = new VaultProperties();
        badProps.setEnabled(true);
        badProps.setAddress("http://localhost:9999");
        badProps.setAuthMode(VaultProperties.AuthMode.TOKEN);
        badProps.setToken("token");
        VaultSecretProvider badProvider = new SpringVaultSecretProvider(badProps);

        ForgeFlowVaultException ex = assertThrows(ForgeFlowVaultException.class, () -> {
            badProvider.readSecretValue("secret", "forgeflow/test-secret", "my-key");
        });
        assertTrue(ex.getMessage().contains("Failed to read secret"));
    }

    @Test
    void test6_JwtServiceRetrievesKeyFromVault() {
        UserDetails user = User.withUsername("vaultuser@example.com").password("pass").authorities("ROLE_USER").build();
        String token = jwtService.generateToken(user);
        
        assertNotNull(token);
        assertTrue(jwtService.isTokenValid(token, user));
        assertEquals("vaultuser@example.com", jwtService.extractUsername(token));
    }
}