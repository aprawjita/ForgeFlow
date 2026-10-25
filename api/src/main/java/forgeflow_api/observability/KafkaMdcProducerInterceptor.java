package forgeflow_api.observability;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

public class KafkaMdcProducerInterceptor implements ProducerInterceptor<String, String> {

    private static final String TRACE_ID_HEADER = "X-Trace-Id";
    private static final String MDC_TRACE_ID_KEY = "traceId";
    private static final Pattern VALID_TRACE_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9-]{16,36}$");

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String traceId = MDC.get(MDC_TRACE_ID_KEY);

        if (traceId != null && VALID_TRACE_ID_PATTERN.matcher(traceId).matches()) {
            boolean hasHeader = false;
            for (Header header : record.headers()) {
                if (TRACE_ID_HEADER.equals(header.key())) {
                    hasHeader = true;
                    break;
                }
            }
            
            if (!hasHeader) {
                record.headers().add(TRACE_ID_HEADER, traceId.getBytes(StandardCharsets.UTF_8));
            }
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}