package io.github.mpiumakkho.branchsales.consumer.observability;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.config.BranchKafkaProperties;
import io.github.mpiumakkho.branchsales.consumer.listener.BranchListeners;

/**
 * Health of this instance's connections to its branches (component {@code branches} of /actuator/health).
 * DOWN when the registry has branches for this shard and none of them is connected; otherwise UP, with the branches
 * that could not be connected listed in the details so one failing branch is visible without taking the instance out
 * of service.
 */
@Component("branches")
public class BranchConnectionsHealthIndicator implements HealthIndicator {

	private final BranchListeners listeners;
	private final BranchKafkaProperties properties;

	public BranchConnectionsHealthIndicator(BranchListeners listeners, BranchKafkaProperties properties) {
		this.listeners = listeners;
		this.properties = properties;
	}

	@Override
	public Health health() {
		int registered = listeners.registeredBranches();
		int connected = listeners.connectedBranches().size();
		Health.Builder health = registered > 0 && connected == 0 ? Health.down() : Health.up();
		return health
				.withDetail("shard", properties.shard())
				.withDetail("registered", registered)
				.withDetail("connected", connected)
				.withDetail("unreachable", listeners.unreachableBranches())
				.withDetail("lastRegistryRefresh", listeners.lastRefresh().map(Object::toString).orElse("never"))
				.build();
	}
}
