/*
 * Copyright 2024-2025 NetCracker Technology Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.qubership.integration.platform.engine.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.camel.model.RouteDefinition;
import org.qubership.integration.platform.engine.model.deployment.engine.EngineDeployment;
import org.qubership.integration.platform.engine.model.deployment.update.DeploymentUpdate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Getter
@NoArgsConstructor
public class RuntimeIntegrationCache {
    @Getter(AccessLevel.NONE)
    private final ConcurrentMap<String, Lock> chainLocks = new ConcurrentHashMap<>(); // <chainId, lock>

    private final ConcurrentMap<String, EngineDeployment> deployments = new ConcurrentHashMap<>(); // <deploymentId, deployment>
    private final ConcurrentMap<String, List<RouteDefinition>> deploymentRoutes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<String>> deploymentSystemModelIds = new ConcurrentHashMap<>();

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private final AtomicReference<Collection<DeploymentUpdate>> deploymentsToRetry =
        new AtomicReference<>(new LinkedList<>());

    public Lock getLockForChain(String chainId) {
        return chainLocks.computeIfAbsent(chainId, (key) -> new ReentrantLock(true /* for FIFO tasks order */));
    }

    public Collection<DeploymentUpdate> flushDeploymentsToRetry() {
        if (deploymentsToRetry.get().isEmpty()) {
            return Collections.emptyList();
        }
        return deploymentsToRetry.getAndSet(new LinkedList<>());
    }

    public void putToRetryQueue(DeploymentUpdate deployment) {
        deploymentsToRetry.updateAndGet(collection -> {
            collection.add(deployment);
            return collection;
        });
    }

    public void removeRetryDeploymentFromQueue(String deploymentId) {
        deploymentsToRetry.updateAndGet(collection -> {
            collection.removeIf(deployment -> deploymentId.equals(deployment.getDeploymentInfo().getDeploymentId()));
            return collection;
        });
    }

    public void cleanForDeployment(String deploymentId) {
        //deployments.remove(deploymentId); // TODO check do we need it
        deploymentRoutes.remove(deploymentId);
        deploymentSystemModelIds.remove(deploymentId);
    }

    public Optional<String> getDeploymentIdByRouteId(String routeId) {
        for (Map.Entry<String, List<RouteDefinition>> routesByDeployment : deploymentRoutes.entrySet()) {
            List<RouteDefinition> routes = routesByDeployment.getValue();
            if (routes.stream().anyMatch(route -> route.getRouteId().equals(routeId))) {
                return Optional.of(routesByDeployment.getKey());
            }
        }
        return Optional.empty();
    }
}
