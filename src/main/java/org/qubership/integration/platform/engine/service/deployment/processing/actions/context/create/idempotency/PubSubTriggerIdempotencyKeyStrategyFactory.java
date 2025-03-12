package org.qubership.integration.platform.engine.service.deployment.processing.actions.context.create.idempotency;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategyBuilder;
import org.qubership.integration.platform.engine.model.ChainElementType;
import org.qubership.integration.platform.engine.model.ElementOptions;
import org.qubership.integration.platform.engine.model.deployment.update.DeploymentInfo;
import org.qubership.integration.platform.engine.model.deployment.update.ElementProperties;
import org.springframework.stereotype.Component;

@Component
public class PubSubTriggerIdempotencyKeyStrategyFactory extends IdempotencyKeyStrategyFactoryBase {
    @Override
    public Collection<ChainElementType> getElementTypes() {
        return Set.of(ChainElementType.PUBSUB_TRIGGER);
    }

    @Override
    protected void configureStrategy(
        IdempotentRepositoryKeyStrategyBuilder builder,
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    ) {
        Map<String, String> props = properties.getProperties();
        builder
            .append("pubsub:")
            .append(props.get(ElementOptions.PROJECT_ID))
            .append(":")
            .append(props.get(ElementOptions.DESTINATION_NAME));
    }
}
