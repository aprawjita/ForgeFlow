package forgeflow_api.vault;

public class ForgeFlowVaultException extends RuntimeException {
    public ForgeFlowVaultException(String message) {
        super(message);
    }
    public ForgeFlowVaultException(String message, Throwable cause) {
        super(message, cause);
    }
}