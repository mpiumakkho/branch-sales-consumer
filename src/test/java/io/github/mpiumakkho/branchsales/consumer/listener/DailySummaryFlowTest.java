package io.github.mpiumakkho.branchsales.consumer.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.JsonNode;

import io.github.mpiumakkho.branchsales.consumer.ContractExamples;
import io.github.mpiumakkho.branchsales.consumer.TestBranch;
import io.github.mpiumakkho.branchsales.consumer.TestcontainersConfiguration;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.repository.DeadLetterStore;

/**
 * Sends the contract example files through the Kafka brokers of two branches and checks what ends up in the HQ
 * database, in dead_letter, and in the receipts each branch gets back.
 */
@SpringBootTest
@ActiveProfiles("test")
@Import(TestcontainersConfiguration.class)
class DailySummaryFlowTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final LocalDate SALE_DATE = LocalDate.of(2026, 10, 1);

	@Autowired
	JdbcClient jdbc;

	@Autowired
	BranchListeners listeners;

	@Autowired
	DeadLetterStore deadLetters;

	@Autowired
	@Qualifier("br0001Kafka")
	KafkaContainer br0001Kafka;

	@Autowired
	@Qualifier("br0002Kafka")
	KafkaContainer br0002Kafka;

	private TestBranch br0001;
	private TestBranch br0002;

	@BeforeEach
	void setUp() {
		jdbc.sql("delete from branch_daily_return").update();
		jdbc.sql("delete from branch_daily_sales").update();
		jdbc.sql("delete from dead_letter").update();
		br0001 = TestBranch.open("BR0001", br0001Kafka);
		br0002 = TestBranch.open("BR0002", br0002Kafka);
		br0001.register(jdbc);
		br0002.register(jdbc);
		await().atMost(TIMEOUT).until(() -> listeners.connectedBranches().containsAll(List.of("BR0001", "BR0002")));
	}

	@AfterEach
	void tearDown() {
		br0001.close();
		br0002.close();
	}

	@Test
	void storesValidExamplesAndRejectsInvalidOnesWithReceiptsToTheBranch() {
		// unknown-branch.json (BR9999) is not here: a branch outside the registry is never read (see contract/README.md)
		Map<String, RejectReason> invalid = new LinkedHashMap<>();
		invalid.put("invalid-schema/amount-as-number.json", RejectReason.SCHEMA_INVALID);
		invalid.put("invalid-schema/empty-lines.json", RejectReason.SCHEMA_INVALID);
		invalid.put("invalid-schema/negative-amount.json", RejectReason.SCHEMA_INVALID);
		invalid.put("invalid-schema/revision-zero.json", RejectReason.SCHEMA_INVALID);
		invalid.put("invalid-business/total-mismatch.json", RejectReason.TOTAL_MISMATCH);
		invalid.put("invalid-business/duplicate-category.json", RejectReason.DUPLICATE_CATEGORY);
		invalid.put("invalid-business/unknown-category.json", RejectReason.UNKNOWN_CATEGORY);
		byte[] notJson = "not json".getBytes(StandardCharsets.UTF_8);

		// Valid and invalid records interleaved on BR0001, so they share batches
		Map<Long, String> sent = new HashMap<>();
		Map<String, Long> offsetOfExample = new HashMap<>();
		sent.put(br0001.send(ContractExamples.read("valid/basic.json")), "INSERTED");
		invalid.forEach((name, reason) -> {
			long offset = br0001.send(ContractExamples.read(name));
			sent.put(offset, reason.name());
			offsetOfExample.put(name, offset);
		});
		sent.put(br0001.send("BR0001", notJson), RejectReason.INVALID_JSON.name());
		long br0002Offset = br0002.send(ContractExamples.read("valid/unknown-field.json"));

		// One receipt per record, matched by source offset
		Map<Long, String> received = new HashMap<>();
		for (JsonNode receipt : br0001.readReceipts(sent.size())) {
			assertThat(receipt.get("branchCode").asString()).isEqualTo("BR0001");
			String outcome = receipt.get("outcome").asString();
			received.put(receipt.get("sourceOffset").asLong(),
					outcome.equals("REJECTED") ? receipt.get("rejectReason").asString() : outcome);
		}
		assertThat(received).isEqualTo(sent);
		JsonNode br0002Receipt = br0002.readReceipts(1).getFirst();
		assertThat(br0002Receipt.get("sourceOffset").asLong()).isEqualTo(br0002Offset);
		assertThat(br0002Receipt.get("outcome").asString()).isEqualTo("INSERTED");

		// Every rejected record is in dead_letter with its bytes unchanged
		assertThat(jdbc.sql("select count(*) from dead_letter").query(Integer.class).single())
				.isEqualTo(invalid.size() + 1);
		invalid.keySet().forEach(name -> assertThat(deadLetterValue("BR0001", offsetOfExample.get(name)))
				.isEqualTo(ContractExamples.read(name)));

		assertThat(storedRevision("BR0001")).contains(1);
		assertThat(lines("BR0001")).containsExactly(
				"BEVERAGE 18200.00 410", "HOUSEHOLD 2500.00 37", "READY_MEAL 9120.50 152", "SNACK 12050.00 395");
		assertThat(lines("BR0002")).containsExactly("OTHER 5000.00 80");
		assertThat(totalAmount("BR0001")).isEqualByComparingTo("41870.50");
	}

	@Test
	void rejectsRecordsWhoseBranchCodeOrKeyDoesNotMatchTheBranch() {
		byte[] br0002Value = ContractExamples.read("valid/unknown-field.json");
		byte[] br0001Value = ContractExamples.read("valid/basic.json");

		// Q5: BR0002's message in BR0001's Kafka (so BR0001's producer wrote it)
		br0001.send("BR0002", br0002Value);
		// Q7: key does not match branchCode, and no key at all
		br0001.send("BR0002", br0001Value);
		br0001.send(null, br0001Value);

		List<JsonNode> receipts = br0001.readReceipts(3);
		assertThat(receipts).extracting(r -> r.get("rejectReason").asString())
				.containsExactly("BRANCH_MISMATCH", "KEY_MISMATCH", "KEY_MISMATCH");
		assertThat(receipts).extracting(r -> r.get("detail").asString()).containsExactly(
				"BRANCH_MISMATCH: branchCode BR0002 read from the Kafka of branch BR0001",
				"KEY_MISMATCH: record key 'BR0002', branchCode BR0001",
				"KEY_MISMATCH: record key missing, branchCode BR0001");
		// The summary could be read, so the receipt names the day and revision for the branch
		assertThat(receipts.get(1).get("saleDate").asString()).isEqualTo("2026-10-01");
		assertThat(receipts.get(1).get("revision").asInt()).isEqualTo(1);
		assertThat(storedRevision("BR0001")).isEmpty();
		assertThat(storedRevision("BR0002")).isEmpty();
	}

	@Test
	void appliesOnlyHigherRevisionsAndTellsTheBranch() {
		br0001.send(ContractExamples.read("valid/basic.json"));
		br0001.send(ContractExamples.read("valid/revision-2.json"));
		// R6 stale, then R5 duplicate
		br0001.send(ContractExamples.read("valid/basic.json"));
		br0001.send(ContractExamples.read("valid/revision-2.json"));

		List<JsonNode> receipts = br0001.readReceipts(4);
		assertThat(receipts).extracting(r -> r.get("outcome").asString() + " " + r.get("revision").asInt() + "->"
				+ r.get("storedRevision").asInt())
				.containsExactly("INSERTED 1->1", "UPDATED 2->2", "STALE 1->2", "DUPLICATE 2->2");

		assertThat(storedRevision("BR0001")).contains(2);
		assertThat(totalAmount("BR0001")).isEqualByComparingTo("42370.50");
		assertThat(eventId("BR0001")).isEqualTo(UUID.fromString("9a7b6c5d-4e3f-4a2b-8c1d-0e9f8a7b6c5d"));
		assertThat(lines("BR0001")).containsExactly(
				"BEVERAGE 18700.00 422", "HOUSEHOLD 2500.00 37", "READY_MEAL 9120.50 152", "SNACK 12050.00 395");
		// Skipped revisions are normal outcomes, not dead letters
		assertThat(jdbc.sql("select count(*) from dead_letter").query(Integer.class).single()).isZero();
	}

	@Test
	void replaysADeadLetterWhenHqAsksForIt() {
		long offset = br0001.send(ContractExamples.read("invalid-business/unknown-category.json"));
		assertThat(br0001.readReceipts(1).getFirst().get("rejectReason").asString()).isEqualTo("UNKNOWN_CATEGORY");

		// HQ adds the missing category, then asks for the record to be processed again
		jdbc.sql("insert into category (category_code, description) values ('LOTTERY', 'Test only')").update();
		try {
			jdbc.sql("update dead_letter set replay_requested_at = now() where branch_code = 'BR0001' and source_offset = ?")
					.param(offset)
					.update();

			JsonNode receipt = br0001.readReceipts(1).getFirst();
			assertThat(receipt.get("sourceOffset").asLong()).isEqualTo(offset);
			assertThat(receipt.get("outcome").asString()).isEqualTo("INSERTED");
			assertThat(lines("BR0001")).containsExactly("LOTTERY 100.00 1");
			await().atMost(TIMEOUT).until(() -> "INSERTED".equals(jdbc
					.sql("select replay_result from dead_letter where branch_code = 'BR0001' and source_offset = ?")
					.param(offset)
					.query(String.class).single()));
			// Done once: no further receipts for it
			assertThat(br0001.pollReceipts(Duration.ofSeconds(2))).isEmpty();
		}
		finally {
			jdbc.sql("delete from branch_daily_sales").update();
			jdbc.sql("delete from category where category_code = 'LOTTERY'").update();
		}
	}

	@Test
	void storesReturnsOfADayWhoseSalesAreAtHq() {
		long salesOffset = br0001.send(ContractExamples.read("valid/basic.json"));
		long returnOffset = br0001.sendReturn(ContractExamples.readReturn("valid/basic.json"));

		List<JsonNode> receipts = br0001.readReceipts(2);
		assertThat(receipts).extracting(r -> r.path("type").asString() + " " + r.get("outcome").asString())
				.containsExactly("DAILY_SUMMARY INSERTED", "DAILY_RETURN INSERTED");
		// Offsets are per topic (both start at 0 in a fresh branch); the receipt's type tells them apart
		assertThat(receipts).extracting(r -> r.get("sourceOffset").asLong()).containsExactly(salesOffset, returnOffset);
		assertThat(receipts.get(1).get("saleDate").asString()).isEqualTo("2026-10-01");
		assertThat(returnLines("BR0001")).containsExactly("BEVERAGE 120.00 3", "HOUSEHOLD 230.00 2");
		assertThat(jdbc.sql("select count(*) from dead_letter").query(Integer.class).single()).isZero();
	}

	@Test
	void holdsAReturnThatArrivesBeforeItsSalesAndReplaysItWhenTheyArrive() {
		long returnOffset = br0001.sendReturn(ContractExamples.readReturn("valid/basic.json"));

		// No sales of 2026-10-01 at HQ yet: kept in dead_letter, the branch is told why, nothing is skipped
		JsonNode rejected = br0001.readReceipts(1).getFirst();
		assertThat(rejected.get("type").asString()).isEqualTo("DAILY_RETURN");
		assertThat(rejected.get("rejectReason").asString()).isEqualTo("PARENT_MISSING");
		assertThat(rejected.get("saleDate").asString()).isEqualTo("2026-10-01");
		assertThat(jdbc.sql("select record_type || ' ' || record_date from dead_letter").query(String.class).single())
				.isEqualTo("DAILY_RETURN 2026-10-01");
		assertThat(returnLines("BR0001")).isEmpty();

		// The sales arrive: HQ stores them and replays the waiting return by itself, with a second receipt
		long salesOffset = br0001.send(ContractExamples.read("valid/basic.json"));
		List<JsonNode> receipts = br0001.readReceipts(2);
		assertThat(receipts).extracting(r -> r.get("type").asString() + " " + r.get("outcome").asString()
				+ " " + r.get("sourceOffset").asLong())
				.containsExactly("DAILY_SUMMARY INSERTED " + salesOffset, "DAILY_RETURN INSERTED " + returnOffset);
		assertThat(returnLines("BR0001")).containsExactly("BEVERAGE 120.00 3", "HOUSEHOLD 230.00 2");
		await().atMost(TIMEOUT).until(() -> "INSERTED".equals(jdbc
				.sql("select replay_result from dead_letter where record_type = 'DAILY_RETURN' and source_offset = ?")
				.param(returnOffset)
				.query(String.class).single()));
		// Done once: a later revision of the sales does not replay it again
		br0001.send(ContractExamples.read("valid/revision-2.json"));
		assertThat(br0001.readReceipts(1).getFirst().get("outcome").asString()).isEqualTo("UPDATED");
		assertThat(br0001.pollReceipts(Duration.ofSeconds(2))).isEmpty();
	}

	@Test
	void requestsTheReplayAgainWhenTheSalesArrivedWhileTheReturnWasBeingRejected() {
		// The listener thread and the replayer can handle one branch at the same time: a return can pass its parent
		// check (no sales yet) and be written to dead_letter after the sales were stored, so the sales transaction's
		// request found no row. The write path asks again when the parent exists by then.
		br0001.send(ContractExamples.read("valid/basic.json"));
		br0001.readReceipts(1);
		byte[] value = ContractExamples.readReturn("valid/basic.json");
		jdbc.sql("""
				insert into dead_letter (branch_code, record_type, source_offset, record_key, record_value, reject_reason,
				  detail, record_date)
				values ('BR0001', 'DAILY_RETURN', 7, 'BR0001', ?, 'PARENT_MISSING', 'PARENT_MISSING: raced', '2026-10-01')
				""").param(value).update();

		assertThat(deadLetters.requestReplayIfParentExists("BR0001", 7, SALE_DATE)).isEqualTo(1);

		JsonNode receipt = br0001.readReceipts(1).getFirst();
		assertThat(receipt.get("type").asString() + " " + receipt.get("outcome").asString()
				+ " " + receipt.get("sourceOffset").asLong()).isEqualTo("DAILY_RETURN INSERTED 7");
		assertThat(returnLines("BR0001")).containsExactly("BEVERAGE 120.00 3", "HOUSEHOLD 230.00 2");
		// Without the parent, nothing is requested
		assertThat(deadLetters.requestReplayIfParentExists("BR0001", 7, SALE_DATE.plusDays(1))).isZero();
	}

	@Test
	void followsTheBranchRegistry() {
		// Offboarded: HQ stops reading the branch; records wait in the branch's Kafka
		jdbc.sql("update branch set kafka_bootstrap = null where branch_code = 'BR0002'").update();
		await().atMost(TIMEOUT).until(() -> !listeners.connectedBranches().contains("BR0002"));
		br0002.send(ContractExamples.read("valid/unknown-field.json"));
		assertThat(br0002.pollReceipts(Duration.ofSeconds(3))).isEmpty();
		assertThat(storedRevision("BR0002")).isEmpty();

		// Onboarded again: the waiting record is read, without a consumer restart
		br0002.register(jdbc);
		assertThat(br0002.readReceipts(1).getFirst().get("outcome").asString()).isEqualTo("INSERTED");
		assertThat(storedRevision("BR0002")).contains(1);
	}

	@Test
	void readsOnlyBranchesOfItsOwnShard() {
		// Moved to another consumer instance's shard: this instance disconnects; the record waits for that instance
		jdbc.sql("update branch set shard = 'other' where branch_code = 'BR0002'").update();
		try {
			await().atMost(TIMEOUT).until(() -> !listeners.connectedBranches().contains("BR0002"));
			long offset = br0002.send(ContractExamples.read("valid/unknown-field.json"));
			assertThat(br0002.pollReceipts(Duration.ofSeconds(3))).isEmpty();
			// A replay request for that branch is left to the other instance as well
			jdbc.sql("""
					insert into dead_letter (branch_code, source_offset, record_key, record_value, reject_reason, detail,
					  replay_requested_at)
					values ('BR0002', ?, 'BR0002', ?, 'UNKNOWN_CATEGORY', 'test', now())
					""").params(offset + 1000, ContractExamples.read("valid/unknown-field.json")).update();
			assertThat(br0002.pollReceipts(Duration.ofSeconds(2))).isEmpty();
		}
		finally {
			jdbc.sql("update branch set shard = 'default' where branch_code = 'BR0002'").update();
		}
		// Back in this shard: connected again, the waiting record and the replay are handled
		await().atMost(TIMEOUT).until(() -> listeners.connectedBranches().contains("BR0002"));
		assertThat(br0002.readReceipts(2)).extracting(r -> r.get("outcome").asString())
				.containsExactlyInAnyOrder("INSERTED", "DUPLICATE");
	}

	@Test
	void branchThatCannotBeConnectedYetIsTriedAgain() throws InterruptedException {
		// The branch's host name does not resolve (e.g. its edge is not up yet): not connected, and not remembered as
		// connected, so every refresh tries again (the demo showed a branch stuck after starting before its edge)
		jdbc.sql("""
				insert into branch (branch_code, name, kafka_bootstrap) values ('BR0003', 'Test branch 3', 'kafka.br0003.invalid:9094')
				on conflict (branch_code) do update set kafka_bootstrap = excluded.kafka_bootstrap
				""").update();
		try {
			Thread.sleep(1500); // a few refreshes
			assertThat(listeners.connectedBranches()).doesNotContain("BR0003").contains("BR0001", "BR0002");
		}
		finally {
			jdbc.sql("update branch set kafka_bootstrap = null where branch_code = 'BR0003'").update();
		}
	}

	private byte[] deadLetterValue(String branchCode, long offset) {
		return jdbc.sql("select record_value from dead_letter where branch_code = ? and source_offset = ?")
				.params(branchCode, offset)
				.query((rs, n) -> rs.getBytes(1))
				.single();
	}

	private Optional<Integer> storedRevision(String branchCode) {
		return jdbc.sql("select revision from branch_daily_sales where branch_code = ? and sale_date = ?")
				.params(branchCode, SALE_DATE)
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

	private List<String> returnLines(String branchCode) {
		return jdbc.sql("""
				select l.category_code || ' ' || l.amount || ' ' || l.quantity
				  from branch_daily_return_line l
				  join branch_daily_return h on h.id = l.branch_daily_return_id
				 where h.branch_code = ? and h.return_date = ?
				 order by l.category_code
				""")
				.params(branchCode, SALE_DATE)
				.query(String.class)
				.list();
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
