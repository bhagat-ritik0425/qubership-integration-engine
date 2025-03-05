package org.qubership.integration.platform.engine.service.deployment.processing.actions.context.create;

import java.util.function.Function;

import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.spring.SpringCamelContext;
import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyParameters;
import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategy;
import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategyBuilder;
import org.qubership.integration.platform.engine.camel.idempotency.RedisIdempotentRepository;
import org.qubership.integration.platform.engine.model.ChainElementType;
import org.qubership.integration.platform.engine.model.constants.CamelConstants.ChainProperties;
import org.qubership.integration.platform.engine.model.deployment.update.DeploymentInfo;
import org.qubership.integration.platform.engine.model.deployment.update.ElementProperties;
import org.qubership.integration.platform.engine.service.deployment.processing.ElementProcessingAction;
import org.qubership.integration.platform.engine.service.deployment.processing.qualifiers.OnAfterDeploymentContextCreated;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
@OnAfterDeploymentContextCreated
public class IdempotentConsumerDependencyBinder extends ElementProcessingAction {
    // private final Function<String, IdempotentRepository> idempotentRepositoryFactory;
    private final RedisTemplate<String, String> redisTemplate;
    
    @Autowired
    public IdempotentConsumerDependencyBinder(
        RedisTemplate<String, String> redisTemplate
        // ,
        // Function<String, IdempotentRepository> idempotentRepositoryFactory
    ) {
        // this.idempotentRepositoryFactory = idempotentRepositoryFactory;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean applicableTo(ElementProperties properties) {
        String elementType = properties.getProperties().get(ChainProperties.ELEMENT_TYPE);
        ChainElementType chainElementType = ChainElementType.fromString(elementType);
        return ChainElementType.IDEMPOTET_CONSUMER.equals(chainElementType);
    }

    @Override
    public void apply(SpringCamelContext context, ElementProperties properties, DeploymentInfo deploymentInfo) {
        String elementId = properties.getElementId();
        String consumerKey = elementId;
        IdempotentRepositoryKeyStrategy keyStrategy = new IdempotentRepositoryKeyStrategyBuilder()
            .append("dupcheck:")
            .append(consumerKey)
            .append(":")
            .appendIdempotencyKey()
            .build();
        IdempotentRepositoryKeyParameters keyParameters = IdempotentRepositoryKeyParameters.builder()
            // FIXME TTL
            .ttl(600)
            .keyStrategy(keyStrategy)
            .build();
        RedisIdempotentRepository idempotentRepository = new RedisIdempotentRepository(redisTemplate, keyParameters);
        // IdempotentRepository idempotentRepository = idempotentRepositoryFactory.apply(consumerKey);
        context.getRegistry().bind(elementId, idempotentRepository);
    }

}
