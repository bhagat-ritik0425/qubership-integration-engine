package org.qubership.integration.platform.engine.camel.idempotency;

public class DuplicateException extends Exception {
    public DuplicateException(String message) {
        super(message);
    }
}
