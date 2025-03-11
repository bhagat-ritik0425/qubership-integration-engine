package org.qubership.integration.platform.engine.camel.idempotency;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.component.redis.processor.idempotent.RedisStringIdempotentRepository;
import org.qubership.integration.platform.engine.service.debugger.util.MessageHelper;
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

    public RedisIdempotentRepository(
        RedisTemplate<String, String> redisTemplate,
        ObjectMapper objectMapper,
        IdempotentRepositoryParameters keyParameters
    ) {
        super(redisTemplate, null);
        setExpiry(keyParameters.getTtl());
        this.objectMapper = objectMapper;
        this.keyParameters = keyParameters;
        this.valueOperations = redisTemplate.opsForValue();
    }

    @Override
    protected String createRedisKey(String idempotencyKey) {
        return keyParameters.getKeyStrategy().buildRepositoryKey(idempotencyKey);
    }

    @Override
    public boolean add(Exchange exchange, String key) {
        String value = createRedisValue(exchange);
        if (keyParameters.getTtl() > 0) {
            Duration expiry = Duration.ofSeconds(keyParameters.getTtl());
            return valueOperations.setIfAbsent(createRedisKey(key), value, expiry);
        }
        return valueOperations.setIfAbsent(createRedisKey(key), value);
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
