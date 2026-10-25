package forgeflow_api.observability;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaMdcPropagationUnitTest {

    private final KafkaMdcProducerInterceptor producerInterceptor = new KafkaMdcProducerInterceptor();
    private final KafkaMdcRecordInterceptor consumerInterceptor = new KafkaMdcRecordInterceptor();

    @AfterEach
    void cleanup() {
        MDC.clear();
    }

    @Test
    void testProducerInjectsValidMdcTraceId() {
        MDC.put("traceId", "valid-trace-1234567890123");
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");
        producerInterceptor.onSend(record);
        assertNotNull(record.headers().lastHeader("X-Trace-Id"));
        assertEquals("valid-trace-1234567890123", new String(record.headers().lastHeader("X-Trace-Id").value(), StandardCharsets.UTF_8));
    }

    @Test
    void testProducerDoesNotOverwriteExistingHeader() {
        MDC.put("traceId", "new-mdc-trace-1234567890");
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");
        record.headers().add("X-Trace-Id", "existing-trace-1234567".getBytes(StandardCharsets.UTF_8));
        producerInterceptor.onSend(record);
        assertEquals("existing-trace-1234567", new String(record.headers().lastHeader("X-Trace-Id").value(), StandardCharsets.UTF_8));
    }

    @Test
    void testConsumerExtractsValidHeaderToMdc() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        record.headers().add(new RecordHeader("X-Trace-Id", "valid-trace-1234567890123".getBytes(StandardCharsets.UTF_8)));
        consumerInterceptor.intercept(record, null);
        assertEquals("valid-trace-1234567890123", MDC.get("traceId"));
    }

    @Test
    void testConsumerGeneratesFallbackIdWithoutMutatingHeaders() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("topic", 0, 0, "key", "value");
        consumerInterceptor.intercept(record, null);
        String generatedId = MDC.get("traceId");
        assertNotNull(generatedId);
        assertTrue(Pattern.matches("^[a-zA-Z0-9-]{16,36}$", generatedId));
        assertNull(record.headers().lastHeader("X-Trace-Id"), "Headers should not be mutated");
    }

    @Test
    void testConsumerClearsMdcOnSuccessAndFailure() {
        MDC.put("traceId", "test-id-success");
        consumerInterceptor.success(new ConsumerRecord<>("topic", 0, 0, "key", "value"), null);
        assertNull(MDC.get("traceId"));

        MDC.put("traceId", "test-id-failure");
        consumerInterceptor.failure(new ConsumerRecord<>("topic", 0, 0, "key", "value"), new RuntimeException("Test"), null);
        assertNull(MDC.get("traceId"));
    }
}