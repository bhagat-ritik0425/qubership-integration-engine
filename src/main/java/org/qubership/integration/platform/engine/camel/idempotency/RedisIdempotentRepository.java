package org.qubership.integration.platform.engine.camel.idempotency;

import java.time.Duration;
import java.util.Set;

import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.support.service.ServiceSupport;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

public class RedisIdempotentRepository extends ServiceSupport implements IdempotentRepository {
    private final RedisTemplate<String, String> redisTemplate;
    private final ValueOperations<String, String> operations;
    private final IdempotentRepositoryKeyParameters keyParameters;

    public RedisIdempotentRepository(
        RedisTemplate<String, String> redisTemplate,
        IdempotentRepositoryKeyParameters keyParameters
    ) {
        this.operations = redisTemplate.opsForValue();
        this.redisTemplate = redisTemplate;
        this.keyParameters = keyParameters;
    }

    @Override
    public boolean add(String key) {
        return operations.setIfAbsent(buildRedisKey(key), "", Duration.ofSeconds(keyParameters.getTtl()));
    }

    @Override
    public boolean contains(String key) {
        return redisTemplate.hasKey(buildRedisKey(key));
    }

    @Override
    public boolean remove(String key) {
        return redisTemplate.delete(buildRedisKey(key));
    }

    @Override
    public boolean confirm(String key) {
        return true;
    }

    @Override
    public void clear() {
        String pattern = keyParameters.getKeyStrategy().getRepositoryKeyPattern();
        Set<String> keys = redisTemplate.keys(pattern);
        redisTemplate.delete(keys);
    }

    private String buildRedisKey(String idempotencyKey) {
        return keyParameters.getKeyStrategy().buildRepositoryKey(idempotencyKey);
    }
}
