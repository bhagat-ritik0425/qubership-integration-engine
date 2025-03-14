package org.qubership.integration.platform.engine.camel.idempotency;

import static java.util.Objects.nonNull;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BiConsumer;

import org.apache.camel.Exchange;
import org.apache.camel.component.redis.processor.idempotent.RedisStringIdempotentRepository;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RedisIdempotentRepository extends RedisStringIdempotentRepository {
    private final IdempotentRepositoryParameters keyParameters;
    private final ObjectMapper objectMapper;
    private final ValueOperations<String, String> valueOperations;
    private final BiConsumer<String, Exchange> onKeyAdd;

    public RedisIdempotentRepository(
        RedisTemplate<String, String> redisTemplate,
        ObjectMapper objectMapper,
        IdempotentRepositoryParameters keyParameters,
        BiConsumer<String, Exchange> onKeyAdd
    ) {
        super(redisTemplate, null);
        setExpiry(keyParameters.getTtl());
        this.objectMapper = objectMapper;
        this.keyParameters = keyParameters;
        this.onKeyAdd = onKeyAdd;
        this.valueOperations = redisTemplate.opsForValue();
    }

    @Override
    protected String createRedisKey(String idempotencyKey) {
        return keyParameters.getKeyStrategy().buildRepositoryKey(idempotencyKey);
    }

    @Override
    public boolean add(Exchange exchange, String key) {
        String redisKey = createRedisKey(key);
        String value = createRedisValue(exchange);
        if (nonNull(onKeyAdd)) {
            onKeyAdd.accept(redisKey, exchange);
        }
        if (keyParameters.getTtl() > 0) {
            Duration expiry = Duration.ofSeconds(keyParameters.getTtl());
            return valueOperations.setIfAbsent(redisKey, value, expiry);
        }
        return valueOperations.setIfAbsent(redisKey, value);
    }

    protected String createRedisValue(Exchange exchange) {
        IdempotencyRecord record = IdempotencyRecord.builder()
            .status(IdempotencyRecordStatus.RECEIVED)
            .createdAt(Timestamp.from(Instant.now()))
            .build();
        try {
            return objectMapper.writeValueAsString(record);
        } catch (JsonProcessingException exception) {
            log.error("Failed to create idempotency Redis value", exception);
            return null;
        }
    }

    @Override
    public void clear() {
        // We are not deleting keys on stop.
    }
}
