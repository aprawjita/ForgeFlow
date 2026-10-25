package forgeflow_api.observability;

import net.logstash.logback.mask.MaskingJsonGeneratorDecorator;
import net.logstash.logback.mask.RegexValueMasker;
import java.util.regex.Pattern;

public class SecretMaskingDecorator extends MaskingJsonGeneratorDecorator {
    public SecretMaskingDecorator() {
        super();
        // Mask URLs containing inline credentials. Replaces Capture Group 1 (the password).
        addValueMasker(new RegexValueMasker(
            Pattern.compile("(?i)(?<=https?://[a-zA-Z0-9_.-]{1,100}:)[^@]+(?=@)"), 
            "***"
        ));

        // Mask generic key-value secrets. Replaces Capture Group 1 (the secret value).
        addValueMasker(new RegexValueMasker(
            Pattern.compile("(?i)(?<=(?:password|secret|token|api_key|apikey|credentials?|authorization)\\s{0,5}[:=]\\s{0,5}['\"]?)[^\\s'\",]+"), 
            "***"
        ));
    }
}