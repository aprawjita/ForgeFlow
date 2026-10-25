package forgeflow_api.observability;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import forgeflow_api.kafka.KafkaProducerService;
import forgeflow_api.worker.DistributedLockService;
import forgeflow_api.worker.StepExecutionEngine;
import forgeflow_api.workflow.JobRunDao;
import forgeflow_api.workflow.StepRunDao;
import forgeflow_api.workflow.WorkflowJobDao;
import forgeflow_api.workflow.WorkflowRunDao;
import net.logstash.logback.encoder.LogstashEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.containers.GenericContainer;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestPropertySource(properties = {
    "management.health.kafka.enabled=false",
    "management.endpoint.health.group.readiness.include=readinessState,db,redis",
    "spring.kafka.bootstrap-servers=localhost:9092"
})
public class StructuredLoggingIntegrationTest {

    static final GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static final GenericContainer<?> postgres = new GenericContainer<>("postgres:15-alpine")
            .withExposedPorts(5432).withEnv("POSTGRES_USER", "testuser").withEnv("POSTGRES_PASSWORD", "testpass").withEnv("POSTGRES_DB", "forgeflow_db");

    static { redis.start(); postgres.start(); }

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

    private ListAppender<ILoggingEvent> listAppender;
    private Logger logger;
    private LogstashEncoder encoder;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setup() {
        logger = (Logger) LoggerFactory.getLogger(StructuredLoggingIntegrationTest.class);
        listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        ch.qos.logback.core.Appender<ILoggingEvent> consoleAppender = rootLogger.getAppender("CONSOLE");
        assertNotNull(consoleAppender, "CONSOLE appender MUST be present and loaded from logback-spring.xml");
        
        encoder = (LogstashEncoder) ((ch.qos.logback.core.OutputStreamAppender<ILoggingEvent>) consoleAppender).getEncoder();
        assertNotNull(encoder, "LogstashEncoder MUST be securely bound to the CONSOLE appender");
    }

    @AfterEach
    void teardown() {
        logger.detachAppender(listAppender);
        MDC.clear();
    }

    private String encodeEvent(Level level, String message, Throwable ex) {
        LoggingEvent event = new LoggingEvent("fqcn", logger, level, message, ex, null);
        return new String(encoder.encode(event), StandardCharsets.UTF_8);
    }

    @Test
    void testJsonLoggingStructureAndMdc() throws Exception {
        MDC.put("traceId", "test-trace-123");
        String jsonStr = encodeEvent(Level.INFO, "Test message for JSON serialization", null);

        assertTrue(jsonStr.startsWith("{") && jsonStr.endsWith("\n"), "Output must be single-line valid JSON");
        
        JsonNode json = objectMapper.readTree(jsonStr);
        assertEquals("Test message for JSON serialization", json.get("message").asText());
        assertEquals("test-trace-123", json.get("traceId").asText(), "MDC traceId must be included at root level");
    }

    @Test
    void testMaskingInLogsAndExceptions() throws Exception {
        // 1. Verify standard log messages
        String jsonStr1 = encodeEvent(Level.INFO, "Connecting to https://admin:SUPER_SECRET_PASS@github.com/repo", null);
        String jsonStr2 = encodeEvent(Level.INFO, "Using configuration password=MyHiddenPassword123", null);

        JsonNode json1 = objectMapper.readTree(jsonStr1);
        JsonNode json2 = objectMapper.readTree(jsonStr2);

        assertFalse(json1.get("message").asText().contains("SUPER_SECRET_PASS"), "URL password should be masked");
        assertTrue(json1.get("message").asText().contains("https://admin:***@github.com/repo"), "Masking token should replace URL password explicitly");

        assertFalse(json2.get("message").asText().contains("MyHiddenPassword123"), "Key-value password should be masked");
        assertTrue(json2.get("message").asText().contains("password=***"), "Masking token should replace KV explicitly");

        // 2. Verify stack traces natively scrubbed
        Exception ex = new RuntimeException("Failure at https://admin:SUPER_SECRET_PASS@github.com/repo");
        String jsonEx = encodeEvent(Level.ERROR, "System crashed", ex);
        JsonNode json3 = objectMapper.readTree(jsonEx);

        String stackTrace = json3.get("stack_trace").asText();
        assertFalse(stackTrace.contains("SUPER_SECRET_PASS"), "Stack trace MUST be natively scrubbed by decorator");
        assertTrue(stackTrace.contains("***"), "Stack trace must contain mask token");
    }
}