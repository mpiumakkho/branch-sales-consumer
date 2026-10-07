package io.github.mpiumakkho.branchsales.consumer.listener;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.backoff.ExponentialBackOff;

import io.github.mpiumakkho.branchsales.consumer.config.BranchKafkaProperties;
import io.github.mpiumakkho.branchsales.consumer.kafka.BranchKafkaClients;
import io.github.mpiumakkho.branchsales.consumer.repository.BranchRegistry;

/**
 * One listener container (and one receipt producer) per branch of this instance's shard in the branch registry. The
 * registry is read again on a fixed delay: a newly onboarded branch is connected, an offboarded one (or one moved to
 * another shard) is disconnected, and a changed address is reconnected, without a restart.
 * <p>
 * Each branch has its own container and thread, so a branch that is offline or slow does not hold up the others.
 */
@Component
public class BranchListeners implements DisposableBean {

	private static final Logger log = LoggerFactory.getLogger(BranchListeners.class);

	private record Connection(String bootstrap, KafkaMessageListenerContainer<String, byte[]> container,
			DefaultKafkaProducerFactory<String, byte[]> producerFactory, KafkaTemplate<String, byte[]> receipts) {
	}

	private final BranchRegistry registry;
	private final BranchKafkaClients clients;
	private final BranchKafkaProperties properties;
	private final DailySummaryListener listener;
	private final Map<String, Connection> connections = new ConcurrentHashMap<>();
	// Branches of the last refresh that could not be connected, with the reason (health endpoint)
	private final Map<String, String> unreachable = new ConcurrentHashMap<>();
	private final AtomicInteger registered = new AtomicInteger();
	private final AtomicReference<Instant> lastRefresh = new AtomicReference<>();

	public BranchListeners(BranchRegistry registry, BranchKafkaClients clients, BranchKafkaProperties properties,
			DailySummaryListener listener) {
		this.registry = registry;
		this.clients = clients;
		this.properties = properties;
		this.listener = listener;
	}

	@Scheduled(initialDelay = 0, fixedDelayString = "${branch-sales.branch-kafka.registry-refresh-ms}")
	public synchronized void refresh() {
		Map<String, String> addresses = registry.kafkaAddresses(properties.shard());
		Set<String> stopped = new TreeSet<>();
		for (String branchCode : Set.copyOf(connections.keySet())) {
			String bootstrap = addresses.get(branchCode);
			Connection connection = connections.get(branchCode);
			if (bootstrap == null || !bootstrap.equals(connection.bootstrap())) {
				disconnect(branchCode);
			}
			else if (!connection.container().isRunning()) {
				// The listener stopped itself: the broker refused HQ's credentials (SASL) or the ACL is missing, which
				// spring-kafka treats as fatal. Reported as unreachable and connected again at the next refresh.
				disconnect(branchCode);
				stopped.add(branchCode);
				unreachable.put(branchCode, bootstrap + ": listener stopped (authentication or authorization refused)");
				log.error("Listener of branch {} at {} stopped; reconnecting at the next refresh", branchCode, bootstrap);
			}
		}
		unreachable.keySet().retainAll(addresses.keySet());
		addresses.forEach((branchCode, bootstrap) -> {
			if (!connections.containsKey(branchCode) && !stopped.contains(branchCode)) {
				try {
					connect(branchCode, bootstrap);
					unreachable.remove(branchCode);
				}
				catch (RuntimeException e) {
					// e.g. the branch host name does not resolve or HQ's password for it is not installed yet; tried again
					// at the next refresh
					String cause = NestedExceptionUtils.getMostSpecificCause(e).getMessage();
					unreachable.put(branchCode, bootstrap + ": " + cause);
					log.error("Cannot connect to branch {} at {}: {}", branchCode, bootstrap, cause);
				}
			}
		});
		registered.set(addresses.size());
		lastRefresh.set(Instant.now());
	}

	/** Branches of this shard in the registry at the last refresh. */
	public int registeredBranches() {
		return registered.get();
	}

	/** Branches of the last refresh that could not be connected, with the reason. */
	public Map<String, String> unreachableBranches() {
		return new TreeMap<>(unreachable);
	}

	/** When the registry was last read, or empty before the first refresh. */
	public Optional<Instant> lastRefresh() {
		return Optional.ofNullable(lastRefresh.get());
	}

	/** Branches with a running listener. A listener that stopped itself is not counted (see {@link #refresh}). */
	public Set<String> connectedBranches() {
		Set<String> running = new TreeSet<>();
		connections.forEach((branchCode, connection) -> {
			if (connection.container().isRunning()) {
				running.add(branchCode);
			}
		});
		return running;
	}

	/** The receipt producer of a connected branch. */
	public Optional<KafkaTemplate<String, byte[]>> receipts(String branchCode) {
		return Optional.ofNullable(connections.get(branchCode)).map(Connection::receipts);
	}

	private void connect(String branchCode, String bootstrap) {
		var producerFactory = new DefaultKafkaProducerFactory<>(clients.producerConfig(branchCode, bootstrap),
				new StringSerializer(), new ByteArraySerializer());
		var receipts = new KafkaTemplate<>(producerFactory);
		var consumerFactory = new DefaultKafkaConsumerFactory<>(clients.consumerConfig(branchCode, bootstrap),
				new StringDeserializer(), new ByteArrayDeserializer());

		var containerProperties = new ContainerProperties(properties.summaryTopic());
		containerProperties.setAckMode(ContainerProperties.AckMode.BATCH);
		containerProperties.setMessageListener(
				(BatchMessageListener<String, byte[]>) records -> listener.onBatch(branchCode, receipts, records));
		var container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
		container.setBeanName("branch-" + branchCode);
		container.setCommonErrorHandler(retryForever());

		try {
			// Fails if the branch host name does not resolve (yet); the next refresh tries again
			container.start();
		}
		catch (RuntimeException e) {
			producerFactory.destroy();
			throw e;
		}
		connections.put(branchCode, new Connection(bootstrap, container, producerFactory, receipts));
		log.info("Connected to branch {} at {}", branchCode, bootstrap);
	}

	private void disconnect(String branchCode) {
		Connection connection = connections.remove(branchCode);
		connection.container().stop();
		connection.producerFactory().destroy();
		log.info("Disconnected from branch {} at {}", branchCode, connection.bootstrap());
	}

	/**
	 * Failures that reach the error handler are infrastructure failures (HQ database or branch Kafka not reachable);
	 * contract rejections are handled by the listener. The record is retried with back-off and no attempt limit: the
	 * branch's partition waits instead of skipping data. Records stay in the branch topic for its retention period.
	 */
	private static DefaultErrorHandler retryForever() {
		var backOff = new ExponentialBackOff(1_000, 2.0);
		// Stays well below max.poll.interval.ms (5 minutes), since the consumer thread sleeps between attempts
		backOff.setMaxInterval(60_000);
		return new DefaultErrorHandler(backOff);
	}

	@Override
	public synchronized void destroy() {
		Set.copyOf(connections.keySet()).forEach(this::disconnect);
	}
}
