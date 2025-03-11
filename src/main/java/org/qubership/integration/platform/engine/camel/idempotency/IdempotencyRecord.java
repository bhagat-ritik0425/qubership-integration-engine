package org.qubership.integration.platform.engine.camel.idempotency;

import java.sql.Timestamp;
import java.util.Map;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
@Builder(toBuilder = true)
public class IdempotencyRecord {
    private IdempotencyRecordStatus status;
    private Timestamp createdAt;
}
