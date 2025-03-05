package org.qubership.integration.platform.engine.camel.idempotency;

public interface IdempotentRepositoryKeyStrategy {
    String getRepositoryKeyPattern();
    String buildRepositoryKey(String idempotencyKey);
}
