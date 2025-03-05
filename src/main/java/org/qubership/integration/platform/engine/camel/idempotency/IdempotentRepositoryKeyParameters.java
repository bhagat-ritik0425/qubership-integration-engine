package org.qubership.integration.platform.engine.camel.idempotency;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Builder(toBuilder = true)
@Getter
@Setter
public class IdempotentRepositoryKeyParameters {
    private int ttl;
    private IdempotentRepositoryKeyStrategy keyStrategy;
}
