package forgeflow_api.observability;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;
import java.util.regex.Pattern;

public class CorrelationIdFilter extends OncePerRequestFilter {

    private static final String TRACE_ID_HEADER = "X-Trace-Id";
    private static final String MDC_TRACE_ID_KEY = "traceId";
    
    // Strict validation: Alphanumeric and hyphens only, 16 to 36 characters.
    private static final Pattern VALID_TRACE_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9-]{16,36}$");

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        
        String traceId = request.getHeader(TRACE_ID_HEADER);

        // Treat external correlation IDs as untrusted.
        if (traceId == null || !VALID_TRACE_ID_PATTERN.matcher(traceId).matches()) {
            traceId = UUID.randomUUID().toString();
        }

        // Establish the trace boundary
        MDC.put(MDC_TRACE_ID_KEY, traceId);
        response.setHeader(TRACE_ID_HEADER, traceId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            // Strictly clear the thread-local state to prevent leaks across pooled Tomcat worker threads
            MDC.remove(MDC_TRACE_ID_KEY);
        }
    }
}