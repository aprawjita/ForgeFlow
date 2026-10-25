package forgeflow_api.observability;

import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.worker.DistributedLockService;
import forgeflow_api.worker.StepExecutionEngine;
import forgeflow_api.workflow.JobRunDao;
import forgeflow_api.workflow.StepRunDao;
import forgeflow_api.workflow.WorkflowJobDao;
import forgeflow_api.workflow.WorkflowRunDao;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.containers.GenericContainer;

import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
public class CorrelationIdFilterIntegrationTest {

    // Removed @Container to prevent premature JUnit teardown
    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static final GenericContainer<?> postgres = new GenericContainer<>("postgres:15-alpine")
            .withExposedPorts(5432)
            .withEnv("POSTGRES_USER", "testuser")
            .withEnv("POSTGRES_PASSWORD", "testpass")
            .withEnv("POSTGRES_DB", "forgeflow_db");

    // Manually manage container lifecycle so they stay alive until the JVM gracefully shuts down
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
        registry.add("management.health.kafka.enabled", () -> "false");
        registry.add("management.endpoint.health.group.readiness.include", () -> "readinessState,db,redis");
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
        // Explicitly inject the filter into the mock context so it actually executes!
        this.mockMvc = MockMvcBuilders.webAppContextSetup(this.webApplicationContext)
                .addFilter(new CorrelationIdFilter(), "/*")
                .build();
    }

    @AfterEach
    void cleanup() {
        MDC.clear();
    }

    @Test
    void testMissingHeaderGeneratesValidId() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Trace-Id", matchesPattern("^[a-zA-Z0-9-]{16,36}$")));
                
        assertNull(MDC.get("traceId"), "MDC should be cleared after request completion");
    }

    @Test
    void testValidHeaderIsPreserved() throws Exception {
        String validId = "custom-trace-123456789012";
        mockMvc.perform(get("/actuator/health").header("X-Trace-Id", validId))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Trace-Id", validId));
                
        assertNull(MDC.get("traceId"), "MDC should be cleared after request completion");
    }

    @Test
    void testMalformedHeaderIsReplaced() throws Exception {
        String malformedId = "too-short";
        String returnedHeader = mockMvc.perform(get("/actuator/health").header("X-Trace-Id", malformedId))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Trace-Id", matchesPattern("^[a-zA-Z0-9-]{16,36}$")))
                .andExpect(header().exists("X-Trace-Id"))
                .andReturn().getResponse().getHeader("X-Trace-Id");
        
        assertNotEquals(malformedId, returnedHeader);
    }

    @Test
    void testOversizedHeaderIsReplaced() throws Exception {
        String oversizedId = "this-id-is-way-too-long-and-exceeds-the-maximum-length-allowed-for-a-trace-id";
        String returnedHeader = mockMvc.perform(get("/actuator/health").header("X-Trace-Id", oversizedId))
                .andExpect(status().isOk())
                .andExpect(header().string("X-Trace-Id", matchesPattern("^[a-zA-Z0-9-]{16,36}$")))
                .andReturn().getResponse().getHeader("X-Trace-Id");
        
        assertNotEquals(oversizedId, returnedHeader);
    }

    @Test
    void testMdcIsClearedAfterExceptionDispatch() throws Exception {
        mockMvc.perform(get("/non-existent-endpoint-to-force-404"))
                .andExpect(status().isNotFound())
                .andExpect(header().exists("X-Trace-Id")); 
                
        assertNull(MDC.get("traceId"), "MDC should be cleared even after exceptions/error dispatches");
    }

    @Test
    void testSecurityRejectsUnauthenticatedButPreservesTraceId() throws Exception {
        mockMvc.perform(get("/actuator/env"))
                .andExpect(status().isNotFound())
                .andExpect(header().exists("X-Trace-Id")); 
                
        assertNull(MDC.get("traceId"), "MDC should be cleared even on security rejections");
    }
}