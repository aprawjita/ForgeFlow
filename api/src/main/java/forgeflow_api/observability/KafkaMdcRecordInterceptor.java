package forgeflow_api.observability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaMdcRecordInterceptor implements RecordInterceptor<Object, Object> {

    private static final String TRACE_ID_HEADER = "X-Trace-Id";
    private static final String MDC_TRACE_ID_KEY = "traceId";
    private static final Pattern VALID_TRACE_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9-]{16,36}$");

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        String traceId = null;
        Header header = record.headers().lastHeader(TRACE_ID_HEADER);
        if (header != null && header.value() != null) {
            traceId = new String(header.value(), StandardCharsets.UTF_8);
        }

        if (traceId == null || !VALID_TRACE_ID_PATTERN.matcher(traceId).matches()) {
            traceId = UUID.randomUUID().toString();
            // LIMITATION: Cannot mutate record.headers() here to persist this fallback ID 
            // across in-memory retries because ConsumerRecord headers are read-only.
        }

        MDC.put(MDC_TRACE_ID_KEY, traceId);
        return record;
    }

    @Override
    public void success(ConsumerRecord<Object, Object> record, Consumer<Object, Object> consumer) {
        MDC.remove(MDC_TRACE_ID_KEY);
    }

    @Override
    public void failure(ConsumerRecord<Object, Object> record, Exception exception, Consumer<Object, Object> consumer) {
        MDC.remove(MDC_TRACE_ID_KEY);
    }
}