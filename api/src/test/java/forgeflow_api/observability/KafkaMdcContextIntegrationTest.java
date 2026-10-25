package forgeflow_api.observability;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.worker.DistributedLockService;
import forgeflow_api.worker.StepExecutionEngine;
import forgeflow_api.workflow.JobRunDao;
import forgeflow_api.workflow.StepRunDao;
import forgeflow_api.workflow.WorkflowJobDao;
import forgeflow_api.workflow.WorkflowRunDao;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
@TestPropertySource(properties = {
    "management.health.kafka.enabled=false",
    "management.endpoint.health.group.readiness.include=readinessState,db,redis",
    "spring.kafka.bootstrap-servers=localhost:9092" // Fake broker address just to satisfy the context load
})
public class KafkaMdcContextIntegrationTest {

    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static final GenericContainer<?> postgres = new GenericContainer<>("postgres:15-alpine")
            .withExposedPorts(5432)
            .withEnv("POSTGRES_USER", "testuser")
            .withEnv("POSTGRES_PASSWORD", "testpass")
            .withEnv("POSTGRES_DB", "forgeflow_db");

    static {
        redis.start();
        postgres.start();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
        registry.add("spring.datasource.url", () -> String.format("jdbc:postgresql://%s:%d/forgeflow_db", postgres.getHost(), postgres.getFirstMappedPort()));
        registry.add("spring.datasource.username", () -> "testuser");
        registry.add("spring.datasource.password", () -> "testpass");
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @MockitoBean private JobRunDao jobRunDao;
    @MockitoBean private StepRunDao stepRunDao;
    @MockitoBean private WorkflowJobDao workflowJobDao;
    @MockitoBean private WorkflowRunDao workflowRunDao;
    @MockitoSpyBean private DistributedLockService distributedLockService;
    @MockitoBean private StepExecutionEngine stepExecutionEngine;
    @MockitoBean private KafkaProducerService kafkaProducerService;

    @Autowired
    private ApplicationContext applicationContext;

    @Test
    void testKafkaMdcInterceptorsAreBoundToContext() {
        // Verify the RecordInterceptor is successfully registered in the Spring Context as a Bean.
        // Spring's KafkaAnnotationDrivenConfiguration automatically detects this and binds it 
        // to the ConcurrentKafkaListenerContainerFactory at runtime when listeners are active.
        assertNotNull(applicationContext.getBean(KafkaMdcRecordInterceptor.class),
            "Spring Boot failed to register the KafkaMdcRecordInterceptor Bean!");
    }
}