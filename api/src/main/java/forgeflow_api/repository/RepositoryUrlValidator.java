package forgeflow_api.repository;

import java.net.URI;
import java.net.URISyntaxException;

public class RepositoryUrlValidator {
    public static void validateCanonicalUrl(String cloneUrl) {
        if (cloneUrl == null || cloneUrl.isBlank()) {
            throw new IllegalArgumentException("Clone URL cannot be empty");
        }
        try {
            URI uri = new URI(cloneUrl);
            if (uri.getUserInfo() != null) {
                // Do not print the token in the exception message!
                throw new IllegalArgumentException("Clone URL must not contain embedded credentials. Authenticate private repositories via the dedicated credential configuration.");
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid repository clone URL format.");
        }
    }
}