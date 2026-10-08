package io.github.mpiumakkho.branchsales.consumer.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.OffsetDateTime;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;

import io.github.mpiumakkho.branchsales.consumer.ContractExamples;
import io.github.mpiumakkho.branchsales.consumer.ReceiptSchema;
import io.github.mpiumakkho.branchsales.consumer.config.BranchKafkaProperties;
import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;
import io.github.mpiumakkho.branchsales.consumer.dto.Receipt;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.repository.DailyFiguresStore;
import io.github.mpiumakkho.branchsales.consumer.service.RecordValidator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** Receipt JSON as the branch reads it, without a broker. */
class ReceiptPublisherTest {

	private static final OffsetDateTime PROCESSED_AT = OffsetDateTime.parse("2026-10-01T15:05:04+07:00");

	private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
	private final ReceiptPublisher publisher = new ReceiptPublisher(new BranchKafkaProperties(
			"branch-sales.daily-summary", "branch-sales.daily-return", "branch-sales.shift-close", "branch-sales.receipt",
			"hq", "PLAINTEXT", "hq", null, null, Duration.ofSeconds(1), "default"), meters);
	private final RecordValidator validator = new RecordValidator();

	@Test
	void shiftCloseReceiptNamesTheTerminalAndShift() {
		DailyFigures shift = validator.validate(RecordType.SHIFT_CLOSE, "BR0001", "BR0001",
				ContractExamples.readShift("valid/basic.json"));
		JsonNode receipt = ReceiptSchema.validate(publisher.toJson(Receipt.stored(RecordType.SHIFT_CLOSE, "BR0001", 0,
				shift, new DailyFiguresStore.Result(DailyFiguresStore.Outcome.INSERTED, 1), PROCESSED_AT)));

		assertThat(receipt.get("type").asString()).isEqualTo("SHIFT_CLOSE");
		assertThat(receipt.get("saleDate").asString()).isEqualTo("2026-10-01");
		assertThat(receipt.get("terminalId").asString()).isEqualTo("POS01");
		assertThat(receipt.get("shiftNo").asInt()).isEqualTo(1);
		assertThat(receipt.get("outcome").asString()).isEqualTo("INSERTED");
	}

	@Test
	void rejectedShiftCloseReceiptNamesTheKeyWhenTheRecordCouldBeRead() {
		var rejection = new RejectedMessageException(RejectReason.UNKNOWN_TENDER,
				"tenderType [GIFT_VOUCHER] is not in the tender_type table", validator.validate(RecordType.SHIFT_CLOSE,
						"BR0001", "BR0001", ContractExamples.readShift("invalid-business/unknown-tender.json")));
		JsonNode receipt = ReceiptSchema.validate(publisher.toJson(
				Receipt.rejected(RecordType.SHIFT_CLOSE, "BR0001", 1, rejection, PROCESSED_AT)));
		assertThat(receipt.get("rejectReason").asString()).isEqualTo("UNKNOWN_TENDER");
		assertThat(receipt.get("terminalId").asString()).isEqualTo("POS01");
		assertThat(receipt.get("shiftNo").asInt()).isEqualTo(1);

		// Not readable: no key fields at all
		JsonNode notJson = ReceiptSchema.validate(publisher.toJson(Receipt.rejected(RecordType.SHIFT_CLOSE, "BR0001", 2,
				new RejectedMessageException(RejectReason.INVALID_JSON, "empty value"), PROCESSED_AT)));
		assertThat(notJson.has("saleDate")).isFalse();
		assertThat(notJson.has("terminalId")).isFalse();
		assertThat(notJson.has("shiftNo")).isFalse();
	}

	@Test
	void dailyReceiptHasNoShiftFields() {
		DailyFigures summary = validator.validate(RecordType.DAILY_SUMMARY, "BR0001", "BR0001",
				ContractExamples.read("valid/basic.json"));
		JsonNode receipt = ReceiptSchema.validate(publisher.toJson(Receipt.stored(RecordType.DAILY_SUMMARY, "BR0001", 0,
				summary, new DailyFiguresStore.Result(DailyFiguresStore.Outcome.INSERTED, 1), PROCESSED_AT)));
		assertThat(receipt.has("terminalId")).isFalse();
		assertThat(receipt.has("shiftNo")).isFalse();
		assertThat(receipt.get("saleDate").asString()).isEqualTo("2026-10-01");
	}

	@Test
	void countersOfTheShiftCloseTypeArePreRegistered() {
		assertThat(meters.find("branch_sales.receipts")
				.tags("type", "SHIFT_CLOSE", "outcome", "REJECTED", "reason", "UNKNOWN_TENDER").counter()).isNotNull();
		// 3 types x (4 outcomes + 12 reject reasons)
		assertThat(meters.find("branch_sales.receipts").counters()).hasSize(48);
	}
}
