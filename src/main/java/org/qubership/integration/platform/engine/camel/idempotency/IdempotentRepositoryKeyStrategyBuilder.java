package org.qubership.integration.platform.engine.camel.idempotency;

import static java.util.Objects.isNull;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class IdempotentRepositoryKeyStrategyBuilder {
    private static record BuildContext(String idempotencyKey) {}

    private static class IdempotentRepositoryKeyStrategyImpl implements IdempotentRepositoryKeyStrategy {
        private final List<Function<BuildContext, String>> appenders;

        public IdempotentRepositoryKeyStrategyImpl(List<Function<BuildContext, String>> appenders) {
            this.appenders = appenders;
        }

        @Override
        public String buildRepositoryKey(String idempotencyKey) {
            return buildForContext(new BuildContext(idempotencyKey));
        }

        private String buildForContext(BuildContext context) {
            return appenders.stream()
                    .map(appender -> appender.apply(context))
                    .collect(Collectors.joining());
        }
    }

    private List<Function<BuildContext, String>> appenders;

    public IdempotentRepositoryKeyStrategyBuilder reset() {
        appenders = null;
        return this;
    }

    public IdempotentRepositoryKeyStrategyBuilder append(String string) {
        getAppenders().add(context -> string);
        return this;
    }

    public IdempotentRepositoryKeyStrategyBuilder appendIdempotencyKey() {
        getAppenders().add(BuildContext::idempotencyKey);
        return this;
    }

    public IdempotentRepositoryKeyStrategy build() {
        return new IdempotentRepositoryKeyStrategyImpl(getAppenders());
    }

    private List<Function<BuildContext, String>> getAppenders() {
        if (isNull(appenders)) {
            appenders = new ArrayList<>();
        }
        return appenders;
    }
}
