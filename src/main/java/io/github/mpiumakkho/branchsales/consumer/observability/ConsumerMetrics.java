package io.github.mpiumakkho.branchsales.consumer.observability;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.listener.BranchListeners;
import io.github.mpiumakkho.branchsales.consumer.repository.DeadLetterStore;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Gauges for /actuator/prometheus. The receipt counter is in {@code ReceiptPublisher}; JVM, JDBC pool and HTTP
 * metrics come from Spring Boot. The Kafka clients of the branches are not instrumented: one set of client metrics
 * per branch would be thousands of series.
 * <p>
 * The dead-letter count is read from the database on the replay schedule, not during a scrape, so a scrape never
 * waits for the database; while the database is down the last value stays.
 */
@Component
public class ConsumerMetrics {

	private static final Logger log = LoggerFactory.getLogger(ConsumerMetrics.class);

	private final DeadLetterStore deadLetters;
	private final AtomicLong openDeadLetters = new AtomicLong();

	public ConsumerMetrics(MeterRegistry meters, BranchListeners listeners, DeadLetterStore deadLetters) {
		this.deadLetters = deadLetters;
		Gauge.builder("branch_sales.branches.registered", listeners, BranchListeners::registeredBranches)
				.description("Branches of this shard in the registry at the last refresh")
				.register(meters);
		Gauge.builder("branch_sales.branches.connected", listeners, l -> l.connectedBranches().size())
				.description("Branches with a running listener")
				.register(meters);
		Gauge.builder("branch_sales.branches.unreachable", listeners, l -> l.unreachableBranches().size())
				.description("Registered branches that could not be connected at the last refresh")
				.register(meters);
		Gauge.builder("branch_sales.dead_letters.open", openDeadLetters, AtomicLong::get)
				.description("Rejected records not replayed successfully yet (all shards), read at the replay interval")
				.register(meters);
	}

	@Scheduled(initialDelay = 0, fixedDelayString = "${branch-sales.dead-letter.replay-interval-ms}")
	public void refresh() {
		try {
			openDeadLetters.set(deadLetters.countOpen());
		}
		catch (RuntimeException e) {
			log.warn("Dead-letter count not refreshed: {}", e.getMessage());
		}
	}
}
