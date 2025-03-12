package org.qubership.integration.platform.engine.service.deployment.processing.actions.context.create.idempotency;

import java.util.Collection;

import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategy;
import org.qubership.integration.platform.engine.model.ChainElementType;
import org.qubership.integration.platform.engine.model.deployment.update.DeploymentInfo;
import org.qubership.integration.platform.engine.model.deployment.update.ElementProperties;

public interface IdempotencyKeyStrategyFactory {
    Collection<ChainElementType> getElementTypes();
    IdempotentRepositoryKeyStrategy getStrategy(
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    );
}
