package org.qubership.integration.platform.engine.service.deployment.processing.actions.context.create;

import java.util.function.Function;

import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.spring.SpringCamelContext;
import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryParameters;
import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategy;
import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategyBuilder;
import org.qubership.integration.platform.engine.model.ChainElementType;
import org.qubership.integration.platform.engine.model.constants.CamelConstants.ChainProperties;
import org.qubership.integration.platform.engine.model.deployment.update.DeploymentInfo;
import org.qubership.integration.platform.engine.model.deployment.update.ElementProperties;
import org.qubership.integration.platform.engine.service.deployment.processing.ElementProcessingAction;
import org.qubership.integration.platform.engine.service.deployment.processing.qualifiers.OnAfterDeploymentContextCreated;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@OnAfterDeploymentContextCreated
public class IdempotentConsumerDependencyBinder extends ElementProcessingAction {
    private final Function<IdempotentRepositoryParameters, IdempotentRepository> idempotentRepositoryFactory;
    
    @Autowired
    public IdempotentConsumerDependencyBinder(
        Function<IdempotentRepositoryParameters, IdempotentRepository> idempotentRepositoryFactory
    ) {
        this.idempotentRepositoryFactory = idempotentRepositoryFactory;
    }

    @Override
    public boolean applicableTo(ElementProperties properties) {
        String elementType = properties.getProperties().get(ChainProperties.ELEMENT_TYPE);
        ChainElementType chainElementType = ChainElementType.fromString(elementType);
        return (
            ChainElementType.HTTP_TRIGGER.equals(chainElementType)
            || ChainElementType.KAFKA_TRIGGER_2.equals(chainElementType)
            || ChainElementType.RABBITMQ_TRIGGER_2.equals(chainElementType)
            || ChainElementType.ASYNCAPI_TRIGGER.equals(chainElementType)
        ) && Boolean.valueOf(properties.getProperties().get(ChainProperties.IDEMPOTENCY_ENABLED));
    }

    @Override
    public void apply(SpringCamelContext context, ElementProperties properties, DeploymentInfo deploymentInfo) {
        String elementId = properties.getElementId();
        IdempotentRepositoryParameters keyParameters = IdempotentRepositoryParameters.builder()
            .ttl(Integer.valueOf(properties.getProperties().get(ChainProperties.EXPIRY)))
            .keyStrategy(getKeyStrategy(properties, deploymentInfo))
            .build();
        IdempotentRepository idempotentRepository = idempotentRepositoryFactory.apply(keyParameters);
        context.getRegistry().bind(elementId, idempotentRepository);
    }

    private static IdempotentRepositoryKeyStrategy getKeyStrategy(
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    ) {
        String elementType = properties.getProperties().get(ChainProperties.ELEMENT_TYPE);
        ChainElementType chainElementType = ChainElementType.fromString(elementType);
        IdempotentRepositoryKeyStrategyBuilder builder = new IdempotentRepositoryKeyStrategyBuilder()
            .append("dupcheck:")
            .append(deploymentInfo.getChainId())
            .append(":");
        switch (chainElementType) {
            case ChainElementType.HTTP_TRIGGER -> {
                builder
                    .append("http:")
                    .append(properties.getProperties().get(ChainProperties.METHOD))
                    .append(":")
                    .append(properties.getProperties().get(ChainProperties.PATH));
            }
            case ChainElementType.KAFKA_TRIGGER_2 -> {
                // TODO
            }
            case ChainElementType.RABBITMQ_TRIGGER_2 -> {
                // TODO
            }
            case ChainElementType.ASYNCAPI_TRIGGER -> {
                // TODO
            }
            default -> {}
        }
        return builder
            .append(":")
            .appendIdempotencyKey()
            .build();
    }

}
