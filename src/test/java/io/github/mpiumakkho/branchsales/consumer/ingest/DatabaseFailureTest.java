package io.github.mpiumakkho.branchsales.consumer.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;

import io.github.mpiumakkho.branchsales.consumer.ContractExamples;
import io.github.mpiumakkho.branchsales.consumer.TestcontainersConfiguration;
import io.github.mpiumakkho.branchsales.consumer.store.DailySalesStore;

/**
 * A database failure is not a contract rejection: the record must be retried
 * until it is stored, and must not be skipped or sent to the dead-letter topic.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class DatabaseFailureTest {

	@MockitoSpyBean
	DailySalesStore store;

	@Autowired
	KafkaTemplate<String, byte[]> kafka;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	KafkaContainer kafkaContainer;

	@Value("${branch-sales.kafka.topic}")
	String topic;

	@Value("${branch-sales.kafka.dead-letter-topic}")
	String deadLetterTopic;

	@Test
	void retriesRecordUntilDatabaseAcceptsIt() {
		jdbc.sql("insert into branch (branch_code, name) values ('BR0001', 'Test branch 1') on conflict do nothing").update();
		doThrow(new TransientDataAccessResourceException("simulated: database not reachable"))
				.doCallRealMethod()
				.when(store).apply(any());

		kafka.send(topic, "BR0001", ContractExamples.read("valid/basic.json")).join();

		await().atMost(Duration.ofSeconds(30)).until(() -> jdbc
				.sql("select count(*) from branch_daily_sales where branch_code = 'BR0001'")
				.query(Integer.class).single() == 1);
		verify(store, atLeast(2)).apply(any());
		assertThat(deadLetterEndOffsets()).allMatch(offset -> offset == 0L);
	}

	private List<Long> deadLetterEndOffsets() {
		try (var reader = new KafkaConsumer<>(
				Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()),
				new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
			List<TopicPartition> partitions = IntStream.range(0, TestcontainersConfiguration.PARTITIONS)
					.mapToObj(p -> new TopicPartition(deadLetterTopic, p))
					.toList();
			return List.copyOf(reader.endOffsets(partitions).values());
		}
	}
}
