package forgeflow_api.observability;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.worker.DistributedLockService;
import forgeflow_api.worker.StepExecutionEngine;
import forgeflow_api.workflow.JobRunDao;
import forgeflow_api.workflow.StepRunDao;
import forgeflow_api.workflow.WorkflowJobDao;
import forgeflow_api.workflow.WorkflowRunDao;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.containers.GenericContainer;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@TestPropertySource(properties = {
    "management.health.kafka.enabled=false",
    "management.endpoint.health.group.readiness.include=readinessState,db,redis"
})
public class ActuatorFoundationIntegrationTest {

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

    @Autowired private WebApplicationContext webApplicationContext;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext).build();
    }

    @Test
    void testHealthEndpointExposed() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("\"status\":\"UP\"")));
    }

    @Test
    void testLivenessAndReadinessProbesIsolated() throws Exception {
        mockMvc.perform(get("/actuator/health/liveness")).andExpect(status().isOk());
        mockMvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk());

        AvailabilityChangeEvent.publish(webApplicationContext, ReadinessState.REFUSING_TRAFFIC);

        try {
            mockMvc.perform(get("/actuator/health/liveness"))
                    .andExpect(status().isOk())
                    .andExpect(content().string(containsString("\"status\":\"UP\"")));

            mockMvc.perform(get("/actuator/health/readiness"))
                    .andExpect(status().is5xxServerError())
                    .andExpect(content().string(containsString("\"status\":\"OUT_OF_SERVICE\"")));
        } finally {
            AvailabilityChangeEvent.publish(webApplicationContext, ReadinessState.ACCEPTING_TRAFFIC);
        }
    }

    @Test
    void testUnauthenticatedCallersCannotSeeDetails() throws Exception {
        String response = mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        
        assertFalse(response.contains("components"), "Unauthenticated probes must not expose component details");
    }

    @Test
    void testPrometheusEndpointExposedAndContainsStandardMetrics() throws Exception {
        mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("jvm_memory_used_bytes")))
                .andExpect(content().string(containsString("process_cpu_usage")));
    }

    @Test
    void testPrometheusEndpointDoesNotLeakSecrets() throws Exception {
        String responseBody = mockMvc.perform(get("/actuator/prometheus"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString().toLowerCase();

        assertFalse(responseBody.contains("secret"), "Metrics must not leak 'secret'");
        assertFalse(responseBody.contains("password"), "Metrics must not leak 'password'");
    }

    @Test
    void testSensitiveActuatorEndpointsAreStrictlyHidden() throws Exception {
        mockMvc.perform(get("/actuator/env")).andExpect(status().isNotFound());
        mockMvc.perform(get("/actuator/heapdump")).andExpect(status().isNotFound());
        mockMvc.perform(get("/actuator/threaddump")).andExpect(status().isNotFound());
    }
}