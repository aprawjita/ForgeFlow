package forgeflow_api.webhook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubPushPayload {
    private String ref;
    private String after; // This is the commit SHA
    private GithubRepositoryDto repository;
    private List<GithubCommitDto> commits;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GithubRepositoryDto {
        private Long id;
        private String name;
        
        @JsonProperty("full_name")
        private String fullName;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GithubCommitDto {
        private String id;
        private String message;
    }
}