package forgeflow_api.user;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
    
    // Spring Data JPA magically writes the SQL for this method based on its name!
    Optional<User> findByEmail(String email);
    
}