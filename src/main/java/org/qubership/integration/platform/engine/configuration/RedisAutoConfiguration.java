package org.qubership.integration.platform.engine.configuration;

import java.util.function.Function;

import org.apache.camel.component.redis.processor.idempotent.RedisIdempotentRepository;
import org.apache.camel.spi.IdempotentRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@AutoConfiguration
public class RedisAutoConfiguration {
    @Bean
    RedisTemplate<String, String> redisTemplate(
        RedisConnectionFactory redisConnectionFactory
    ) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        return template;
    }

    @Bean
    @ConditionalOnMissingBean(name = "idempotentRepositoryFactory")
    Function<String, IdempotentRepository> idempotentRepository(
        RedisTemplate<String, String> redisTemplate
    ) {
        return consumerKey -> new RedisIdempotentRepository(redisTemplate, consumerKey);
    }
}
