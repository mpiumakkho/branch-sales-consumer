package io.github.mpiumakkho.branchsales.consumer.listener;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

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

	public BranchListeners(BranchRegistry registry, BranchKafkaClients clients, BranchKafkaProperties properties,
			DailySummaryListener listener) {
		this.registry = registry;
		this.clients = clients;
		this.properties = properties;
		this.listener = listener;
	}

	@Scheduled(initialDelay = 0, fixedDelayString = "${branch-sales.branch-kafka.registry-refresh-ms}")
	public synchronized void refresh() {
		Map<String, String> registered = registry.kafkaAddresses(properties.shard());
		for (String branchCode : Set.copyOf(connections.keySet())) {
			String bootstrap = registered.get(branchCode);
			if (bootstrap == null || !bootstrap.equals(connections.get(branchCode).bootstrap())) {
				disconnect(branchCode);
			}
		}
		registered.forEach((branchCode, bootstrap) -> {
			if (!connections.containsKey(branchCode)) {
				try {
					connect(branchCode, bootstrap);
				}
				catch (RuntimeException e) {
					// e.g. the branch host name does not resolve or HQ's password for it is not installed yet; tried again
					// at the next refresh
					log.error("Cannot connect to branch {} at {}: {}", branchCode, bootstrap,
							NestedExceptionUtils.getMostSpecificCause(e).getMessage());
				}
			}
		});
	}

	/** Branches with a running listener. */
	public Set<String> connectedBranches() {
		return new TreeSet<>(connections.keySet());
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
