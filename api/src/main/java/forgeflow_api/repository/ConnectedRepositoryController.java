package forgeflow_api.repository;

import forgeflow_api.user.User;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/repositories")
@RequiredArgsConstructor
public class ConnectedRepositoryController {

    private final ConnectedRepositoryService service;

    @PostMapping
    public ResponseEntity<ConnectedRepositoryResponse> connectRepository(
            @Valid @RequestBody ConnectRepositoryRequest request,
            @AuthenticationPrincipal User currentUser
    ) {
        return ResponseEntity.ok(service.connectRepository(request, currentUser));
    }

    @GetMapping
    public ResponseEntity<List<ConnectedRepositoryResponse>> getUserRepositories(
            @AuthenticationPrincipal User currentUser
    ) {
        return ResponseEntity.ok(service.getUserRepositories(currentUser));
    }
}