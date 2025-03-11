package org.qubership.integration.platform.engine.camel.idempotency;

public interface IdempotentRepositoryKeyStrategy {
    String buildRepositoryKey(String idempotencyKey);
}
