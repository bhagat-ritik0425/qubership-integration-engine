package org.qubership.integration.platform.engine.service.deployment.processing.actions.context.create.idempotency;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategyBuilder;
import org.qubership.integration.platform.engine.model.ChainElementType;
import org.qubership.integration.platform.engine.model.ElementOptions;
import org.qubership.integration.platform.engine.model.deployment.update.DeploymentInfo;
import org.qubership.integration.platform.engine.model.deployment.update.ElementProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class RabbitMQTriggerIdempotencyKeyStrategyFactory extends IdempotencyKeyStrategyFactoryBase {

    @Override
    protected void configureStrategy(
        IdempotentRepositoryKeyStrategyBuilder builder,
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    ) {
        Map<String, String> props = properties.getProperties();
        builder
            .append("rabbit:")
            .append(props.get(ElementOptions.EXCHANGE))
            .append(":")
            .append(props.get(ElementOptions.QUEUES));
    }

    @Override
    public Collection<ChainElementType> getElementTypes() {
        return Set.of(ChainElementType.RABBITMQ_TRIGGER_2);
    }
}
