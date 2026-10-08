package io.github.mpiumakkho.branchsales.consumer.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
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
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;

import io.github.mpiumakkho.branchsales.consumer.ContractExamples;
import io.github.mpiumakkho.branchsales.consumer.TestBranch;
import io.github.mpiumakkho.branchsales.consumer.TestcontainersConfiguration;

/**
 * Shift closes of POS terminals (contract ShiftClose) through the Kafka brokers of the test branches: stored per
 * (branch, business date, terminal, shift) by revision, with or without the day's sales, and rejected or replayed
 * like the other types.
 */
@SpringBootTest
@ActiveProfiles("test")
@Import(TestcontainersConfiguration.class)
class ShiftCloseFlowTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	@Autowired
	JdbcClient jdbc;

	@Autowired
	BranchListeners listeners;

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
		jdbc.sql("delete from branch_shift_close").update();
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
	void storesShiftsOfTwoTerminalsAndTwoShiftsOfOneDay() {
		long pos01 = br0001.sendShiftClose(ContractExamples.readShift("valid/basic.json"));
		long pos02Shift1 = br0001.sendShiftClose(ContractExamples.readShift("valid/no-cashier.json"));
		long pos02Shift3 = br0001.sendShiftClose(ContractExamples.readShift("valid/night-shift-crossing-midnight.json"));
		br0002.sendShiftClose(ContractExamples.readShift("valid/unknown-field.json"));

		// Receipts are checked against the receipt schema by TestBranch; saleDate carries the businessDate
		List<JsonNode> receipts = br0001.readReceipts(3);
		assertThat(receipts).extracting(r -> r.get("type").asString() + " " + r.get("outcome").asString() + " "
				+ r.get("sourceOffset").asLong() + " " + r.get("saleDate").asString() + " "
				+ r.get("terminalId").asString() + "#" + r.get("shiftNo").asInt())
				.containsExactly(
						"SHIFT_CLOSE INSERTED " + pos01 + " 2026-10-01 POS01#1",
						"SHIFT_CLOSE INSERTED " + pos02Shift1 + " 2026-10-01 POS02#1",
						"SHIFT_CLOSE INSERTED " + pos02Shift3 + " 2026-10-01 POS02#3");
		assertThat(br0002.readReceipts(1).getFirst().get("terminalId").asString()).isEqualTo("T-01");

		// The night shift keeps the business day it was opened under; over/short is computed at HQ
		assertThat(shifts("BR0001")).containsExactly(
				"2026-10-01 POS01 1 r1 C101 18450.00 tx212 over/short -20.00",
				"2026-10-01 POS02 1 r1 - 18450.00 tx212 over/short -20.00",
				"2026-10-01 POS02 3 r1 C107 4210.00 tx58 over/short 0.00");
		assertThat(tenders("BR0001", "POS01", 1))
				.containsExactly("CASH 9120.00 140", "CREDIT_CARD 6230.00 48", "QR_PAYMENT 3100.00 24");
		assertThat(tenders("BR0001", "POS02", 3)).containsExactly("CASH 2980.00 41", "QR_PAYMENT 1230.00 17");
		assertThat(shifts("BR0002")).containsExactly("2026-10-01 T-01 1 r1 C101 1500.00 tx30 over/short 0.00");
		assertThat(jdbc.sql("select count(*) from dead_letter").query(Integer.class).single()).isZero();
	}

	@Test
	void appliesAReopenedShiftAsAHigherRevisionAndReplacesItsTenders() {
		br0001.sendShiftClose(ContractExamples.readShift("valid/no-cashier.json"));
		br0001.sendShiftClose(ContractExamples.readShift("valid/basic.json"));
		br0001.readReceipts(2);
		List<Long> tenderIdsBefore = tenderIds("POS01", 1);

		// Reopened at the POS and closed again: revision 2 of the same key, the cash recounted
		br0001.sendShiftClose(ContractExamples.readShift("valid/reopened-revision-2.json"));
		// R5 duplicate, then R6 stale
		br0001.sendShiftClose(ContractExamples.readShift("valid/reopened-revision-2.json"));
		br0001.sendShiftClose(ContractExamples.readShift("valid/basic.json"));

		List<JsonNode> receipts = br0001.readReceipts(3);
		assertThat(receipts).extracting(r -> r.get("outcome").asString() + " " + r.get("revision").asInt() + "->"
				+ r.get("storedRevision").asInt() + " " + r.get("terminalId").asString() + "#" + r.get("shiftNo").asInt())
				.containsExactly("UPDATED 2->2 POS01#1", "DUPLICATE 2->2 POS01#1", "STALE 1->2 POS01#1");

		assertThat(shifts("BR0001")).containsExactly(
				"2026-10-01 POS01 1 r2 C101 18450.00 tx212 over/short 0.00",
				"2026-10-01 POS02 1 r1 - 18450.00 tx212 over/short -20.00");
		assertThat(jdbc.sql("""
				select event_id from branch_shift_close where branch_code = 'BR0001' and terminal_id = 'POS01' and shift_no = 1
				""").query(UUID.class).single()).isEqualTo(UUID.fromString("7a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"));
		// The tender lines of the higher revision replaced the stored ones (new rows, same content here)
		assertThat(tenders("BR0001", "POS01", 1))
				.containsExactly("CASH 9120.00 140", "CREDIT_CARD 6230.00 48", "QR_PAYMENT 3100.00 24");
		assertThat(tenderIds("POS01", 1)).hasSize(3).doesNotContainAnyElementsOf(tenderIdsBefore);
		assertThat(jdbc.sql("select count(*) from dead_letter").query(Integer.class).single()).isZero();
	}

	@Test
	void storesAShiftWithoutTheDaySales() {
		// Shifts close during the day, the day's summary is confirmed in the evening: no parent rule (R12)
		br0001.sendShiftClose(ContractExamples.readShift("valid/basic.json"));
		JsonNode receipt = br0001.readReceipts(1).getFirst();
		assertThat(receipt.get("outcome").asString()).isEqualTo("INSERTED");
		assertThat(jdbc.sql("select count(*) from branch_daily_sales").query(Integer.class).single()).isZero();
		assertThat(jdbc.sql("select count(*) from dead_letter").query(Integer.class).single()).isZero();

		// The summary arriving later is stored on its own, and nothing is replayed for the shift
		br0001.send(ContractExamples.read("valid/basic.json"));
		assertThat(br0001.readReceipts(1).getFirst().get("type").asString()).isEqualTo("DAILY_SUMMARY");
		assertThat(br0001.pollReceipts(Duration.ofSeconds(2))).isEmpty();
		assertThat(shifts("BR0001")).hasSize(1);
	}

	@Test
	void storesAShiftWithNoTransactions() {
		br0001.sendShiftClose(ContractExamples.readShift("valid/no-transactions.json"));
		assertThat(br0001.readReceipts(1).getFirst().get("outcome").asString()).isEqualTo("INSERTED");
		assertThat(shifts("BR0001")).containsExactly("2026-10-01 POS03 2 r1 C112 0.00 tx0 over/short 0.00");
		assertThat(tenders("BR0001", "POS03", 2)).isEmpty();
	}

	@Test
	void rejectsUnknownTenderAndReplaysAfterItIsAdded() {
		long offset = br0001.sendShiftClose(ContractExamples.readShift("invalid-business/unknown-tender.json"));
		JsonNode rejected = br0001.readReceipts(1).getFirst();
		assertThat(rejected.get("type").asString()).isEqualTo("SHIFT_CLOSE");
		assertThat(rejected.get("rejectReason").asString()).isEqualTo("UNKNOWN_TENDER");
		assertThat(rejected.get("detail").asString())
				.isEqualTo("UNKNOWN_TENDER: tenderType [GIFT_VOUCHER] is not in the tender_type table");
		assertThat(rejected.get("terminalId").asString() + "#" + rejected.get("shiftNo").asInt()).isEqualTo("POS01#1");
		assertThat(jdbc.sql("select record_type || ' ' || record_date || ' ' || reject_reason from dead_letter")
				.query(String.class).single()).isEqualTo("SHIFT_CLOSE 2026-10-01 UNKNOWN_TENDER");
		assertThat(shifts("BR0001")).isEmpty();

		// HQ adds the tender type to its master, then asks for the record to be processed again
		jdbc.sql("insert into tender_type (tender_type, description) values ('GIFT_VOUCHER', 'Test only')").update();
		try {
			jdbc.sql("""
					update dead_letter set replay_requested_at = now()
					 where branch_code = 'BR0001' and record_type = 'SHIFT_CLOSE' and source_offset = ?
					""").param(offset).update();

			JsonNode receipt = br0001.readReceipts(1).getFirst();
			assertThat(receipt.get("type").asString() + " " + receipt.get("outcome").asString() + " "
					+ receipt.get("sourceOffset").asLong()).isEqualTo("SHIFT_CLOSE INSERTED " + offset);
			assertThat(tenders("BR0001", "POS01", 1))
					.containsExactly("CASH 9120.00 140", "CREDIT_CARD 6230.00 48", "GIFT_VOUCHER 3100.00 24");
			await().atMost(TIMEOUT).until(() -> "INSERTED".equals(jdbc
					.sql("select replay_result from dead_letter where record_type = 'SHIFT_CLOSE' and source_offset = ?")
					.param(offset)
					.query(String.class).single()));
		}
		finally {
			jdbc.sql("delete from branch_shift_close").update();
			jdbc.sql("delete from tender_type where tender_type = 'GIFT_VOUCHER'").update();
		}
	}

	@Test
	void rejectsClosedBeforeOpened() {
		br0001.sendShiftClose(ContractExamples.readShift("invalid-business/closed-before-opened.json"));
		br0001.sendShiftClose(ContractExamples.readShift("invalid-business/duplicate-tender.json"));
		br0001.sendShiftClose(ContractExamples.readShift("invalid-schema/terminal-id-with-hash.json"));

		List<JsonNode> receipts = br0001.readReceipts(3);
		assertThat(receipts).extracting(r -> r.get("rejectReason").asString())
				.containsExactly("SHIFT_TIMES_INVALID", "DUPLICATE_TENDER", "SCHEMA_INVALID");
		// Readable records name their key; a schema-invalid one has no key fields
		assertThat(receipts.getFirst().get("terminalId").asString()).isEqualTo("POS01");
		assertThat(receipts.getFirst().get("shiftNo").asInt()).isEqualTo(1);
		assertThat(receipts.get(2).has("terminalId")).isFalse();
		assertThat(jdbc.sql("select count(*) from dead_letter where record_type = 'SHIFT_CLOSE'")
				.query(Integer.class).single()).isEqualTo(3);
		assertThat(shifts("BR0001")).isEmpty();
	}

	@Test
	void branchWithoutTheShiftTopicIsReadPartiallyAndReconnectedWhenItAppears() {
		// A broker of its own: the shared test brokers already have the shift topic
		try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"))) {
			kafka.start();
			try (TestBranch br0009 = TestBranch.openWithout("BR0009", kafka, TestBranch.SHIFT_TOPIC)) {
				br0009.register(jdbc);
				try {
					// Not yet upgraded: read for the topics it has
					await().atMost(TIMEOUT).until(() -> listeners.connectedBranches().contains("BR0009"));
					br0009.send(asBranch("BR0009", ContractExamples.read("valid/basic.json")));
					assertThat(br0009.readReceipts(1).getFirst().get("outcome").asString()).isEqualTo("INSERTED");

					// Its kafka-init adds the shift topic: HQ reconnects within a registry refresh and reads it too
					br0009.createTopic(TestBranch.SHIFT_TOPIC);
					br0009.sendShiftClose(asBranch("BR0009", ContractExamples.readShift("valid/basic.json")));
					JsonNode receipt = br0009.readReceipts(1).getFirst();
					assertThat(receipt.get("type").asString() + " " + receipt.get("outcome").asString())
							.isEqualTo("SHIFT_CLOSE INSERTED");
					assertThat(shifts("BR0009")).hasSize(1);
				}
				finally {
					jdbc.sql("update branch set kafka_bootstrap = null where branch_code = 'BR0009'").update();
					await().atMost(TIMEOUT).until(() -> !listeners.connectedBranches().contains("BR0009"));
				}
			}
		}
	}

	/** The example value as sent by another branch. */
	private static byte[] asBranch(String branchCode, byte[] value) {
		return new String(value, StandardCharsets.UTF_8).replace("\"BR0001\"", "\"" + branchCode + "\"")
				.getBytes(StandardCharsets.UTF_8);
	}

	private List<String> shifts(String branchCode) {
		return jdbc.sql("""
				select business_date || ' ' || terminal_id || ' ' || shift_no || ' r' || revision || ' '
				       || coalesce(cashier_id, '-') || ' ' || total_amount || ' tx' || transaction_count
				       || ' over/short ' || cash_over_short
				  from branch_shift_close
				 where branch_code = ?
				 order by business_date, terminal_id, shift_no
				""")
				.params(branchCode)
				.query(String.class)
				.list();
	}

	private List<String> tenders(String branchCode, String terminalId, int shiftNo) {
		return jdbc.sql("""
				select t.tender_type || ' ' || t.amount || ' ' || t.quantity
				  from branch_shift_close_tender t
				  join branch_shift_close h on h.id = t.branch_shift_close_id
				 where h.branch_code = ? and h.terminal_id = ? and h.shift_no = ?
				 order by t.tender_type
				""")
				.params(branchCode, terminalId, shiftNo)
				.query(String.class)
				.list();
	}

	private List<Long> tenderIds(String terminalId, int shiftNo) {
		return jdbc.sql("""
				select t.id
				  from branch_shift_close_tender t
				  join branch_shift_close h on h.id = t.branch_shift_close_id
				 where h.branch_code = 'BR0001' and h.terminal_id = ? and h.shift_no = ?
				""")
				.params(terminalId, shiftNo)
				.query(Long.class)
				.list();
	}
}
