package io.github.mpiumakkho.branchsales.consumer.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;

import io.github.mpiumakkho.branchsales.consumer.ContractExamples;
import io.github.mpiumakkho.branchsales.consumer.TestBranch;
import io.github.mpiumakkho.branchsales.consumer.TestcontainersConfiguration;
import io.github.mpiumakkho.branchsales.consumer.repository.DailyFiguresStore;

/**
 * A database failure is not a contract rejection: the record must be retried until it is stored. It must not be
 * skipped, kept as a dead letter, or answered with a REJECTED receipt.
 */
@SpringBootTest
@ActiveProfiles("test")
@Import(TestcontainersConfiguration.class)
class DatabaseFailureTest {

	@MockitoSpyBean
	DailyFiguresStore store;

	@Autowired
	JdbcClient jdbc;

	@Autowired
	BranchListeners listeners;

	@Autowired
	@Qualifier("br0001Kafka")
	KafkaContainer br0001Kafka;

	@Test
	void retriesRecordUntilDatabaseAcceptsIt() {
		try (TestBranch br0001 = TestBranch.open("BR0001", br0001Kafka)) {
			br0001.register(jdbc);
			await().atMost(Duration.ofSeconds(30)).until(() -> listeners.connectedBranches().contains("BR0001"));
			doThrow(new TransientDataAccessResourceException("simulated: database not reachable"))
					.doCallRealMethod()
					.when(store).apply(any());

			long offset = br0001.send(ContractExamples.read("valid/basic.json"));

			List<JsonNode> receipts = br0001.readReceipts(1);
			assertThat(receipts.getFirst().get("sourceOffset").asLong()).isEqualTo(offset);
			assertThat(receipts.getFirst().get("outcome").asString()).isEqualTo("INSERTED");
			assertThat(br0001.pollReceipts(Duration.ofSeconds(2))).isEmpty();
			verify(store, atLeast(2)).apply(any());
			assertThat(jdbc.sql("select count(*) from branch_daily_sales where branch_code = 'BR0001'")
					.query(Integer.class).single()).isEqualTo(1);
			assertThat(jdbc.sql("select count(*) from dead_letter").query(Integer.class).single()).isZero();
		}
	}
}
