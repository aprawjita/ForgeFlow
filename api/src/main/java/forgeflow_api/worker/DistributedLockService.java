package forgeflow_api.worker;

public interface DistributedLockService {
    /**
     * Attempts to acquire a distributed lock.
     * Fails fast if the lock is already held by another process.
     * 
     * @param lockKey The unique identifier for the lock.
     * @return true if the lock was acquired, false otherwise.
     */
    boolean tryLock(String lockKey);

    /**
     * Releases the lock safely.
     * Ensures only the thread that holds the lock can release it.
     * 
     * @param lockKey The unique identifier for the lock.
     */
    void unlock(String lockKey);
}