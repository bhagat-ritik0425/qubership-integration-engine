package org.qubership.integration.platform.engine.camel.idempotency;

import org.apache.camel.component.redis.processor.idempotent.RedisStringIdempotentRepository;
import org.springframework.data.redis.core.RedisTemplate;

public class RedisIdempotentRepository extends RedisStringIdempotentRepository {
    private final IdempotentRepositoryKeyParameters keyParameters;

    public RedisIdempotentRepository(
        RedisTemplate<String, String> redisTemplate,
        IdempotentRepositoryKeyParameters keyParameters
    ) {
        super(redisTemplate, null);
        setExpiry(keyParameters.getTtl());
        this.keyParameters = keyParameters;
    }

    @Override
    protected String createRedisKey(String idempotencyKey) {
        return keyParameters.getKeyStrategy().buildRepositoryKey(idempotencyKey);
    }
}
