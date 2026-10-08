package io.github.mpiumakkho.branchsales.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;

/**
 * One test branch: its Kafka broker with the two contract topics, a producer acting as the branch producer, and a
 * reader for the receipts HQ writes back. The receipt reader starts at the end of the topic when the branch is opened.
 */
public final class TestBranch implements AutoCloseable {

	public static final String SUMMARY_TOPIC = "branch-sales.daily-summary";
	public static final String RETURN_TOPIC = "branch-sales.daily-return";
	public static final String RECEIPT_TOPIC = "branch-sales.receipt";

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	private final String code;
	private final KafkaContainer kafka;
	private final KafkaProducer<String, byte[]> producer;
	private final KafkaConsumer<String, byte[]> receiptReader;

	private TestBranch(String code, KafkaContainer kafka) {
		this.code = code;
		this.kafka = kafka;
		createTopics();
		this.producer = new KafkaProducer<>(Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
				ProducerConfig.ACKS_CONFIG, "all"),
				new StringSerializer(), new ByteArraySerializer());
		this.receiptReader = new KafkaConsumer<>(Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
				new StringDeserializer(), new ByteArrayDeserializer());
		var partition = new TopicPartition(RECEIPT_TOPIC, 0);
		receiptReader.assign(List.of(partition));
		receiptReader.seekToEnd(List.of(partition));
		receiptReader.position(partition); // resolve the end offset now, before the test sends anything
	}

	public static TestBranch open(String code, KafkaContainer kafka) {
		return new TestBranch(code, kafka);
	}

	public String code() {
		return code;
	}

	public String bootstrap() {
		return kafka.getBootstrapServers();
	}

	/** Adds or updates the branch in the HQ branch registry with this broker's address. */
	public void register(JdbcClient jdbc) {
		jdbc.sql("""
				insert into branch (branch_code, name, kafka_bootstrap) values (:code, :name, :bootstrap)
				on conflict (branch_code) do update set kafka_bootstrap = excluded.kafka_bootstrap
				""")
				.param("code", code)
				.param("name", "Test branch " + code)
				.param("bootstrap", bootstrap())
				.update();
	}

	/** Sends as the branch producer would: key = the branchCode in the value. @return the record's offset */
	public long send(byte[] value) {
		return send(ContractExamples.branchCodeOf(value), value);
	}

	/** @return the record's offset in the summary topic */
	public long send(String key, byte[] value) {
		return send(SUMMARY_TOPIC, key, value);
	}

	/** Sends a return as the branch producer would. @return the record's offset in the return topic */
	public long sendReturn(byte[] value) {
		return send(RETURN_TOPIC, ContractExamples.branchCodeOf(value), value);
	}

	/** @return the record's offset in that topic */
	public long send(String topic, String key, byte[] value) {
		try {
			return producer.send(new ProducerRecord<>(topic, key, value)).get().offset();
		}
		catch (InterruptedException | ExecutionException e) {
			throw new IllegalStateException(e);
		}
	}

	/** Reads exactly {@code count} new receipts, each checked against the receipt schema. */
	public List<JsonNode> readReceipts(int count) {
		// Polled on the test thread: KafkaConsumer is single-threaded and Awaitility evaluates conditions on its own thread
		List<JsonNode> receipts = new ArrayList<>();
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (receipts.size() < count && System.nanoTime() < deadline) {
			receiptReader.poll(Duration.ofMillis(500)).forEach(record -> {
				assertThat(record.key()).isEqualTo(code);
				receipts.add(ReceiptSchema.validate(record.value()));
			});
		}
		assertThat(receipts).as("receipts of %s", code).hasSize(count);
		return receipts;
	}

	/** Receipts that arrive within {@code wait}, for checking that none come. */
	public List<JsonNode> pollReceipts(Duration wait) {
		List<JsonNode> receipts = new ArrayList<>();
		receiptReader.poll(wait).forEach(record -> receipts.add(ReceiptSchema.validate(record.value())));
		return receipts;
	}

	private void createTopics() {
		try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
			for (String topic : Set.of(SUMMARY_TOPIC, RETURN_TOPIC, RECEIPT_TOPIC)) {
				try {
					admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
				}
				catch (ExecutionException e) {
					if (!(e.getCause() instanceof TopicExistsException)) {
						throw new IllegalStateException(e);
					}
				}
			}
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void close() {
		producer.close();
		receiptReader.close();
	}
}
