package org.qubership.integration.platform.engine.configuration;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

@AutoConfiguration
public class RedisAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    RedisTemplate<String, Object> redisTemplate(
        RedisConnectionFactory redisConnectionFactory
    ) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        return template;
    }
}
