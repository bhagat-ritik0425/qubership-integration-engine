package org.qubership.integration.platform.engine.service.deployment.processing.actions.context.create.idempotency;

import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategy;
import org.qubership.integration.platform.engine.camel.idempotency.IdempotentRepositoryKeyStrategyBuilder;
import org.qubership.integration.platform.engine.model.deployment.update.DeploymentInfo;
import org.qubership.integration.platform.engine.model.deployment.update.ElementProperties;

public abstract class IdempotencyKeyStrategyFactoryBase implements IdempotencyKeyStrategyFactory {
    @Override
    public IdempotentRepositoryKeyStrategy getStrategy(
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    ) {
        IdempotentRepositoryKeyStrategyBuilder builder = new IdempotentRepositoryKeyStrategyBuilder();

        configurePrefix(builder, properties, deploymentInfo);
        configureStrategy(builder, properties, deploymentInfo);
        configureSuffix(builder, properties, deploymentInfo);

        return builder.build();
    }

    protected abstract void configureStrategy(
        IdempotentRepositoryKeyStrategyBuilder builder,
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    );

    private void configurePrefix(
        IdempotentRepositoryKeyStrategyBuilder builder,
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    ) {
        builder
            .append("dupcheck:")
            .append(deploymentInfo.getChainId())
            .append(":");
    }

    private void configureSuffix(
        IdempotentRepositoryKeyStrategyBuilder builder,
        ElementProperties properties,
        DeploymentInfo deploymentInfo
    ) {
        builder
            .append(":")
            .appendIdempotencyKey();
    }
}
