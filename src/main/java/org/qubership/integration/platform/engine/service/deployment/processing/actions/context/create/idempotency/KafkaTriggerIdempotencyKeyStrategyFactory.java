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

@Component()
@Order(Ordered.LOWEST_PRECEDENCE)
public class KafkaTriggerIdempotencyKeyStrategyFactory extends IdempotencyKeyStrategyFactoryBase {
    @Override
    public Collection<ChainElementType> getElementTypes() {
        return Set.of(ChainElementType.KAFKA_TRIGGER_2);
    }

    @Override
    protected void configureStrategy(
        IdempotentRepositoryKeyStrategyBuilder builder,
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    ) {
        Map<String, String> props = properties.getProperties();
        builder
            .append("kafka:")
            .append(props.get(ElementOptions.TOPICS))
            .append(":")
            .append(getGroupId(props));
    }

    private String getGroupId(Map<String, String> props) {
        return props.get(ElementOptions.GROUP_ID); // TODO
    }
}
