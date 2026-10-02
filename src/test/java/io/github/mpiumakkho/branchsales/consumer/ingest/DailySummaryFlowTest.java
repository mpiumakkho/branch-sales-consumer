package io.github.mpiumakkho.branchsales.consumer.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.kafka.KafkaContainer;

import io.github.mpiumakkho.branchsales.consumer.ContractExamples;
import io.github.mpiumakkho.branchsales.consumer.TestcontainersConfiguration;
import io.github.mpiumakkho.branchsales.consumer.validation.RejectReason;

/**
 * Sends the contract example files through Kafka and checks what ends up in
 * the HQ database and in the dead-letter topic.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class DailySummaryFlowTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final LocalDate SALE_DATE = LocalDate.of(2026, 10, 1);

	@Autowired
	KafkaTemplate<String, byte[]> kafka;

	@Autowired
	JdbcClient jdbc;

	// Not ${spring.kafka.bootstrap-servers}: @ServiceConnection does not set that property, so it would resolve to the
	// application.yaml default (the local infra Kafka) instead of the test container
	@Autowired
	KafkaContainer kafkaContainer;

	@Value("${branch-sales.kafka.topic}")
	String topic;

	@Value("${branch-sales.kafka.dead-letter-topic}")
	String deadLetterTopic;

	private KafkaConsumer<String, byte[]> deadLetterReader;

	@BeforeEach
	void setUp() {
		// Branch registry as assumed by contract/README.md: BR0001 and BR0002 exist, BR9999 does not
		jdbc.sql("""
				insert into branch (branch_code, name) values ('BR0001', 'Test branch 1'), ('BR0002', 'Test branch 2')
				on conflict do nothing
				""").update();
		jdbc.sql("delete from branch_daily_sales").update();
		deadLetterReader = openDeadLetterReaderAtEnd();
	}

	@AfterEach
	void tearDown() {
		deadLetterReader.close();
	}

	@Test
	void storesValidExamplesAndSendsInvalidOnesToDeadLetterTopicWithReason() {
		Map<String, RejectReason> invalid = Map.of(
				"invalid-schema/amount-as-number.json", RejectReason.SCHEMA_INVALID,
				"invalid-schema/empty-lines.json", RejectReason.SCHEMA_INVALID,
				"invalid-schema/negative-amount.json", RejectReason.SCHEMA_INVALID,
				"invalid-schema/revision-zero.json", RejectReason.SCHEMA_INVALID,
				"invalid-business/total-mismatch.json", RejectReason.TOTAL_MISMATCH,
				"invalid-business/duplicate-category.json", RejectReason.DUPLICATE_CATEGORY,
				"invalid-business/unknown-branch.json", RejectReason.UNKNOWN_BRANCH,
				"invalid-business/unknown-category.json", RejectReason.UNKNOWN_CATEGORY);
		byte[] notJson = "not json".getBytes(StandardCharsets.UTF_8);

		// Valid and invalid records interleaved, so they share batches
		send("BR0001", ContractExamples.read("valid/basic.json"));
		invalid.keySet().forEach(name -> send("BR0001", ContractExamples.read(name)));
		send("BR0001", notJson);
		send("BR0002", ContractExamples.read("valid/unknown-field.json"));

		List<ConsumerRecord<String, byte[]>> deadLetters = readDeadLetters(invalid.size() + 1);

		Map<String, RejectReason> expected = new HashMap<>(invalid);
		expected.put("not json", RejectReason.INVALID_JSON);
		Map<String, RejectReason> actual = new HashMap<>();
		for (ConsumerRecord<String, byte[]> record : deadLetters) {
			actual.put(sourceOf(record.value(), invalid.keySet()), RejectReason.valueOf(header(record, "reject-reason")));
			assertThat(record.key()).isEqualTo("BR0001");
			assertThat(header(record, "kafka_dlt-original-topic")).isEqualTo(topic);
			// Spring Kafka writes the original partition as a 4-byte int
			assertThat(ByteBuffer.wrap(rawHeader(record, "kafka_dlt-original-partition")).getInt())
					.isEqualTo(record.partition());
		}
		assertThat(actual).isEqualTo(expected);

		await().atMost(TIMEOUT).until(() -> storedRevision("BR0002").isPresent());
		assertThat(storedRevision("BR0001")).contains(1);
		assertThat(lines("BR0001")).containsExactly(
				"BEVERAGE 18200.00 410", "HOUSEHOLD 2500.00 37", "READY_MEAL 9120.50 152", "SNACK 12050.00 395");
		assertThat(lines("BR0002")).containsExactly("OTHER 5000.00 80");
		assertThat(totalAmount("BR0001")).isEqualByComparingTo("41870.50");
	}

	@Test
	void appliesOnlyHigherRevisions() {
		send("BR0001", ContractExamples.read("valid/basic.json"));
		await().atMost(TIMEOUT).until(() -> storedRevision("BR0001").equals(Optional.of(1)));

		// R4: higher revision replaces header and lines
		send("BR0001", ContractExamples.read("valid/revision-2.json"));
		await().atMost(TIMEOUT).until(() -> storedRevision("BR0001").equals(Optional.of(2)));
		assertThat(lines("BR0001")).containsExactly(
				"BEVERAGE 18700.00 422", "HOUSEHOLD 2500.00 37", "READY_MEAL 9120.50 152", "SNACK 12050.00 395");

		// R6 stale, then R5 duplicate. A marker for another day on the same key (same partition, so read after both)
		// shows when the consumer has handled them.
		send("BR0001", ContractExamples.read("valid/basic.json"));
		send("BR0001", ContractExamples.read("valid/revision-2.json"));
		send("BR0001", basicForDate(SALE_DATE.plusDays(1)));
		await().atMost(TIMEOUT).until(() -> storedRevision("BR0001", SALE_DATE.plusDays(1)).isPresent());

		assertThat(storedRevision("BR0001")).contains(2);
		assertThat(totalAmount("BR0001")).isEqualByComparingTo("42370.50");
		assertThat(eventId("BR0001")).isEqualTo(UUID.fromString("9a7b6c5d-4e3f-4a2b-8c1d-0e9f8a7b6c5d"));
		assertThat(lines("BR0001")).hasSize(4);
		// Skipped revisions are normal outcomes, not dead letters
		assertThat(deadLetterReader.poll(Duration.ofSeconds(2))).isEmpty();
	}

	private void send(String key, byte[] value) {
		kafka.send(topic, key, value).join();
	}

	private static byte[] basicForDate(LocalDate date) {
		String json = new String(ContractExamples.read("valid/basic.json"), StandardCharsets.UTF_8);
		return json.replace("\"saleDate\": \"2026-10-01\"", "\"saleDate\": \"" + date + "\"")
				.replace("3f1c2a9e-8b4d-4c1e-9f2a-6d7e8a9b0c1d", UUID.randomUUID().toString())
				.getBytes(StandardCharsets.UTF_8);
	}

	private KafkaConsumer<String, byte[]> openDeadLetterReaderAtEnd() {
		var reader = new KafkaConsumer<>(Map.<String, Object>of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
				new StringDeserializer(), new ByteArrayDeserializer());
		List<TopicPartition> partitions = new ArrayList<>();
		for (int p = 0; p < TestcontainersConfiguration.PARTITIONS; p++) {
			partitions.add(new TopicPartition(deadLetterTopic, p));
		}
		reader.assign(partitions);
		reader.seekToEnd(partitions);
		partitions.forEach(reader::position); // resolve the end offsets now, before the test sends anything
		return reader;
	}

	private List<ConsumerRecord<String, byte[]>> readDeadLetters(int count) {
		// Polled on the test thread: KafkaConsumer is single-threaded and Awaitility evaluates conditions on its own thread
		List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (records.size() < count && System.nanoTime() < deadline) {
			deadLetterReader.poll(Duration.ofMillis(500)).forEach(records::add);
		}
		assertThat(records).as("dead-letter records").hasSize(count);
		return records;
	}

	/** Which example file (or the literal "not json") a dead-letter value came from; also proves the bytes are unchanged. */
	private static String sourceOf(byte[] value, Iterable<String> examples) {
		for (String name : examples) {
			if (Arrays.equals(value, ContractExamples.read(name))) {
				return name;
			}
		}
		return new String(value, StandardCharsets.UTF_8);
	}

	private static String header(ConsumerRecord<?, ?> record, String name) {
		return new String(rawHeader(record, name), StandardCharsets.UTF_8);
	}

	private static byte[] rawHeader(ConsumerRecord<?, ?> record, String name) {
		Header header = record.headers().lastHeader(name);
		assertThat(header).as("header %s", name).isNotNull();
		return header.value();
	}

	private Optional<Integer> storedRevision(String branchCode) {
		return storedRevision(branchCode, SALE_DATE);
	}

	private Optional<Integer> storedRevision(String branchCode, LocalDate saleDate) {
		return jdbc.sql("select revision from branch_daily_sales where branch_code = ? and sale_date = ?")
				.params(branchCode, saleDate)
				.query(Integer.class)
				.optional();
	}

	private BigDecimal totalAmount(String branchCode) {
		return jdbc.sql("select total_amount from branch_daily_sales where branch_code = ? and sale_date = ?")
				.params(branchCode, SALE_DATE)
				.query(BigDecimal.class)
				.single();
	}

	private UUID eventId(String branchCode) {
		return jdbc.sql("select event_id from branch_daily_sales where branch_code = ? and sale_date = ?")
				.params(branchCode, SALE_DATE)
				.query(UUID.class)
				.single();
	}

	private List<String> lines(String branchCode) {
		return jdbc.sql("""
				select l.category_code || ' ' || l.amount || ' ' || l.quantity
				  from branch_daily_sales_line l
				  join branch_daily_sales h on h.id = l.branch_daily_sales_id
				 where h.branch_code = ? and h.sale_date = ?
				 order by l.category_code
				""")
				.params(branchCode, SALE_DATE)
				.query(String.class)
				.list();
	}
}
