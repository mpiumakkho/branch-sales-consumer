package io.github.mpiumakkho.branchsales.consumer.observability;

import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.listener.BranchListeners;
import io.github.mpiumakkho.branchsales.consumer.repository.DeadLetterStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Gauges for /actuator/prometheus. The record counter is in {@code SummaryRecordHandler}; the Kafka client and JVM
 * metrics come from Spring Boot.
 */
@Component
public class ConsumerMetrics {

	public ConsumerMetrics(MeterRegistry meters, BranchListeners listeners, DeadLetterStore deadLetters) {
		Gauge.builder("branch_sales.branches.registered", listeners, BranchListeners::registeredBranches)
				.description("Branches of this shard in the registry at the last refresh")
				.register(meters);
		Gauge.builder("branch_sales.branches.connected", listeners, l -> l.connectedBranches().size())
				.description("Branches with a running listener")
				.register(meters);
		Gauge.builder("branch_sales.branches.unreachable", listeners, l -> l.unreachableBranches().size())
				.description("Registered branches that could not be connected at the last refresh")
				.register(meters);
		// One count query per scrape
		Gauge.builder("branch_sales.dead_letters.open", deadLetters, DeadLetterStore::countOpen)
				.description("Rejected records not replayed successfully yet (all shards)")
				.register(meters);
	}
}
