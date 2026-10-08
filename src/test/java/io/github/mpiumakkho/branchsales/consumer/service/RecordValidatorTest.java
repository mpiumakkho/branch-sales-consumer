package io.github.mpiumakkho.branchsales.consumer.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.github.mpiumakkho.branchsales.consumer.ContractExamples;
import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;
import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures.Line;
import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures.ShiftDetail;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordKey.DayKey;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordKey.ShiftKey;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;

class RecordValidatorTest {

	private static final Map<String, RejectReason> REJECTED = Map.of(
			"invalid-schema/amount-as-number.json", RejectReason.SCHEMA_INVALID,
			"invalid-schema/empty-lines.json", RejectReason.SCHEMA_INVALID,
			"invalid-schema/negative-amount.json", RejectReason.SCHEMA_INVALID,
			"invalid-schema/revision-zero.json", RejectReason.SCHEMA_INVALID,
			"invalid-business/total-mismatch.json", RejectReason.TOTAL_MISMATCH,
			"invalid-business/duplicate-category.json", RejectReason.DUPLICATE_CATEGORY);

	// The unknown-* examples fail only the reference layer, which needs the database (see DailySummaryFlowTest)
	private static final Set<String> ACCEPTED = Set.of(
			"valid/basic.json",
			"valid/revision-2.json",
			"valid/unknown-field.json",
			"invalid-business/unknown-branch.json",
			"invalid-business/unknown-category.json");

	static final List<String> EXAMPLES = ContractExamples.all();

	private final RecordValidator validator = new RecordValidator();

	@Test
	void everyContractExampleHasAnExpectedResult() {
		Set<String> covered = new HashSet<>(ACCEPTED);
		covered.addAll(REJECTED.keySet());
		assertThat(covered).containsExactlyInAnyOrderElementsOf(EXAMPLES);
	}

	@ParameterizedTest
	@FieldSource("EXAMPLES")
	void contractExample(String example) {
		byte[] value = ContractExamples.read(example);
		if (ACCEPTED.contains(example)) {
			assertThat(validate(value)).isNotNull();
		}
		else {
			assertRejected(value, REJECTED.get(example));
		}
	}

	@Test
	void mapsFieldsWithMoneyAsBigDecimal() {
		DailyFigures summary = validate(ContractExamples.read("valid/basic.json"));

		assertThat(summary.eventId()).isEqualTo(UUID.fromString("3f1c2a9e-8b4d-4c1e-9f2a-6d7e8a9b0c1d"));
		assertThat(summary.branchCode()).isEqualTo("BR0001");
		assertThat(summary.type()).isEqualTo(RecordType.DAILY_SUMMARY);
		assertThat(summary.date()).isEqualTo(LocalDate.of(2026, 10, 1));
		assertThat(summary.revision()).isEqualTo(1);
		assertThat(summary.confirmedAt().toString()).isEqualTo("2026-10-01T21:45+07:00");
		assertThat(summary.totalAmount()).isEqualTo(new BigDecimal("41870.50"));
		assertThat(summary.lines()).hasSize(4)
				.first().isEqualTo(new Line("BEVERAGE", new BigDecimal("18200.00"), 410));
	}

	@ParameterizedTest
	@ValueSource(strings = { "not json", "{\"schemaVersion\": 1", "{} {}" })
	void rejectsValueThatIsNotJson(String value) {
		assertRejected(value.getBytes(StandardCharsets.UTF_8), RejectReason.INVALID_JSON);
	}

	// Return examples: accepted by the return schema, or the reason they are rejected (reference checks need the DB)
	private static final Map<String, RejectReason> RETURN_REJECTED = Map.of(
			"invalid-schema/sale-date-instead-of-return-date.json", RejectReason.SCHEMA_INVALID);
	private static final Set<String> RETURN_ACCEPTED = Set.of("valid/basic.json");

	@Test
	void everyReturnExampleHasAnExpectedResult() {
		Set<String> covered = new HashSet<>(RETURN_ACCEPTED);
		covered.addAll(RETURN_REJECTED.keySet());
		assertThat(covered).containsExactlyInAnyOrderElementsOf(ContractExamples.allReturns());
		for (String example : ContractExamples.allReturns()) {
			byte[] value = ContractExamples.readReturn(example);
			if (RETURN_ACCEPTED.contains(example)) {
				assertThat(validator.validate(RecordType.DAILY_RETURN, "BR0001", "BR0001", value)).isNotNull();
			}
			else {
				assertThatThrownBy(() -> validator.validate(RecordType.DAILY_RETURN, "BR0001", "BR0001", value))
						.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RETURN_REJECTED.get(example));
			}
		}
	}

	@Test
	void returnExamplesAreCheckedAgainstTheReturnSchema() {
		DailyFigures figures = validator.validate(RecordType.DAILY_RETURN, "BR0001", "BR0001",
				ContractExamples.readReturn("valid/basic.json"));
		assertThat(figures.type()).isEqualTo(RecordType.DAILY_RETURN);
		assertThat(figures.date()).isEqualTo(LocalDate.of(2026, 10, 1));
		assertThat(figures.totalAmount()).isEqualTo(new BigDecimal("350.00"));

		// returnDate is the date field of a return; a summary sent to the return topic has saleDate instead
		assertThatThrownBy(() -> validator.validate(RecordType.DAILY_RETURN, "BR0001", "BR0001",
				ContractExamples.readReturn("invalid-schema/sale-date-instead-of-return-date.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
		assertThatThrownBy(() -> validator.validate(RecordType.DAILY_RETURN, "BR0001", "BR0001",
				ContractExamples.read("valid/basic.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
		// and a return sent to the summary topic fails the summary schema
		assertThatThrownBy(() -> validate(ContractExamples.readReturn("valid/basic.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
	}

	@Test
	void rejectsEmptyValue() {
		assertRejected(new byte[0], RejectReason.INVALID_JSON);
		assertRejected(null, RejectReason.INVALID_JSON);
	}

	@Test
	void rejectsRepeatedKey() {
		assertRejected(replace("\"revision\": 1,", "\"revision\": 1, \"revision\": 5,"), RejectReason.INVALID_JSON);
	}

	@Test
	void rejectsJsonThatIsNotAnObject() {
		assertRejected("[]".getBytes(StandardCharsets.UTF_8), RejectReason.SCHEMA_INVALID);
	}

	@Test
	void checksFormats() {
		assertRejected(replace("\"2026-10-01\"", "\"2026-02-30\""), RejectReason.SCHEMA_INVALID);
		assertRejected(replace("3f1c2a9e-8b4d-4c1e-9f2a-6d7e8a9b0c1d", "not-a-uuid"), RejectReason.SCHEMA_INVALID);
		assertRejected(replace("2026-10-01T21:45:00+07:00", "2026-10-01 21:45"), RejectReason.SCHEMA_INVALID);
	}

	@Test
	void rejectsRevisionAboveIntRange() {
		assertRejected(replace("\"revision\": 1,", "\"revision\": 3000000000,"), RejectReason.SCHEMA_INVALID);
	}

	@Test
	void rejectsBranchCodeThatIsNotTheClusterBranch() {
		assertThatThrownBy(() -> validator.validate(RecordType.DAILY_SUMMARY, "BR0002", "BR0001",
				ContractExamples.read("valid/basic.json")))
				.isInstanceOf(RejectedMessageException.class)
				.hasMessage("BRANCH_MISMATCH: branchCode BR0001 read from the Kafka of branch BR0002");
	}

	@Test
	void rejectsKeyThatIsNotTheBranchCode() {
		byte[] value = ContractExamples.read("valid/basic.json");
		assertThatThrownBy(() -> validator.validate(RecordType.DAILY_SUMMARY, "BR0001", "BR0002", value))
				.hasMessage("KEY_MISMATCH: record key 'BR0002', branchCode BR0001");
		assertThatThrownBy(() -> validator.validate(RecordType.DAILY_SUMMARY, "BR0001", null, value))
				.hasMessage("KEY_MISMATCH: record key missing, branchCode BR0001");
	}

	@Test
	void identityIsCheckedAfterSchemaAndBeforeBusinessRules() {
		// Schema error wins over a wrong branch
		assertThatThrownBy(() -> validator.validate(RecordType.DAILY_SUMMARY, "BR0002", "BR0001",
				ContractExamples.read("invalid-schema/revision-zero.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
		// A wrong branch wins over a business error
		assertThatThrownBy(() -> validator.validate(RecordType.DAILY_SUMMARY, "BR0002", "BR0001",
				ContractExamples.read("invalid-business/total-mismatch.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.BRANCH_MISMATCH);
	}

	// Shift close examples: accepted by the shift close schema and business rules, or the reason they are rejected.
	// unknown-tender and unknown-branch fail only the reference layer, which needs the database (ShiftCloseFlowTest).
	private static final Map<String, RejectReason> SHIFT_REJECTED = Map.of(
			"invalid-schema/missing-terminal-id.json", RejectReason.SCHEMA_INVALID,
			"invalid-schema/terminal-id-with-hash.json", RejectReason.SCHEMA_INVALID,
			"invalid-schema/shift-no-zero.json", RejectReason.SCHEMA_INVALID,
			"invalid-schema/cash-counted-as-number.json", RejectReason.SCHEMA_INVALID,
			"invalid-schema/tender-type-lowercase.json", RejectReason.SCHEMA_INVALID,
			"invalid-schema/sale-date-instead-of-business-date.json", RejectReason.SCHEMA_INVALID,
			"invalid-business/duplicate-tender.json", RejectReason.DUPLICATE_TENDER,
			"invalid-business/total-mismatch.json", RejectReason.TOTAL_MISMATCH,
			"invalid-business/closed-before-opened.json", RejectReason.SHIFT_TIMES_INVALID);
	private static final Set<String> SHIFT_ACCEPTED = Set.of(
			"valid/basic.json",
			"valid/reopened-revision-2.json",
			"valid/night-shift-crossing-midnight.json",
			"valid/no-transactions.json",
			"valid/no-cashier.json",
			"valid/unknown-field.json",
			"invalid-business/unknown-tender.json",
			"invalid-business/unknown-branch.json");

	static final List<String> SHIFT_EXAMPLES = ContractExamples.allShifts();

	@Test
	void everyShiftExampleHasAnExpectedResult() {
		Set<String> covered = new HashSet<>(SHIFT_ACCEPTED);
		covered.addAll(SHIFT_REJECTED.keySet());
		assertThat(covered).containsExactlyInAnyOrderElementsOf(SHIFT_EXAMPLES);
	}

	@ParameterizedTest
	@FieldSource("SHIFT_EXAMPLES")
	void shiftExample(String example) {
		byte[] value = ContractExamples.readShift(example);
		if (SHIFT_ACCEPTED.contains(example)) {
			assertThat(validate(RecordType.SHIFT_CLOSE, value)).isNotNull();
		}
		else {
			assertThatThrownBy(() -> validate(RecordType.SHIFT_CLOSE, value))
					.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(SHIFT_REJECTED.get(example));
		}
	}

	@Test
	void mapsShiftFieldsIncludingKeyAndCashFigures() {
		DailyFigures shift = validate(RecordType.SHIFT_CLOSE, ContractExamples.readShift("valid/basic.json"));

		assertThat(shift.type()).isEqualTo(RecordType.SHIFT_CLOSE);
		assertThat(shift.key()).isEqualTo(new ShiftKey(LocalDate.of(2026, 10, 1), "POS01", 1));
		assertThat(shift.date()).isEqualTo(LocalDate.of(2026, 10, 1));
		assertThat(shift.revision()).isEqualTo(1);
		assertThat(shift.confirmedAt().toString()).isEqualTo("2026-10-01T15:02:11+07:00");
		assertThat(shift.totalAmount()).isEqualTo(new BigDecimal("18450.00"));
		assertThat(shift.lines()).containsExactly(
				new Line("CASH", new BigDecimal("9120.00"), 140),
				new Line("CREDIT_CARD", new BigDecimal("6230.00"), 48),
				new Line("QR_PAYMENT", new BigDecimal("3100.00"), 24));
		assertThat(shift.shift()).isEqualTo(new ShiftDetail("C101",
				OffsetDateTime.parse("2026-10-01T07:00:00+07:00"), OffsetDateTime.parse("2026-10-01T15:02:11+07:00"), 212,
				new BigDecimal("9120.00"), new BigDecimal("9100.00")));

		assertThat(validate(RecordType.SHIFT_CLOSE, ContractExamples.readShift("valid/no-cashier.json")).shift().cashierId())
				.isNull();
		DailyFigures empty = validate(RecordType.SHIFT_CLOSE, ContractExamples.readShift("valid/no-transactions.json"));
		assertThat(empty.lines()).isEmpty();
		assertThat(empty.totalAmount()).isEqualByComparingTo("0");
		assertThat(empty.shift().transactionCount()).isZero();
		// The daily types have a date key and no shift fields
		DailyFigures summary = validate(ContractExamples.read("valid/basic.json"));
		assertThat(summary.key()).isEqualTo(new DayKey(LocalDate.of(2026, 10, 1)));
		assertThat(summary.shift()).isNull();
	}

	@Test
	void rejectsShiftClosedBeforeOpened() {
		assertThatThrownBy(() -> validate(RecordType.SHIFT_CLOSE,
				ContractExamples.readShift("invalid-business/closed-before-opened.json")))
				.isInstanceOf(RejectedMessageException.class)
				.hasMessage("SHIFT_TIMES_INVALID: closedAt 2026-10-01T06:55+07:00 is before openedAt 2026-10-01T07:00+07:00")
				// The record could be read, so the receipt can name its key
				.extracting(e -> ((RejectedMessageException) e).figures().key())
				.isEqualTo(new ShiftKey(LocalDate.of(2026, 10, 1), "POS01", 1));
	}

	@Test
	void rejectsRepeatedTenderType() {
		assertThatThrownBy(() -> validate(RecordType.SHIFT_CLOSE,
				ContractExamples.readShift("invalid-business/duplicate-tender.json")))
				.isInstanceOf(RejectedMessageException.class)
				.hasMessage("DUPLICATE_TENDER: repeated tenderType [CASH]");
		// The daily types keep their reason and wording
		assertThatThrownBy(() -> validate(ContractExamples.read("invalid-business/duplicate-category.json")))
				.isInstanceOf(RejectedMessageException.class)
				.hasMessageStartingWith("DUPLICATE_CATEGORY: repeated categoryCode [");
	}

	@Test
	void rejectsShiftNoAboveIntRange() {
		String json = new String(ContractExamples.readShift("valid/basic.json"), StandardCharsets.UTF_8);
		assertThat(json).contains("\"shiftNo\": 1,");
		byte[] value = json.replace("\"shiftNo\": 1,", "\"shiftNo\": 3000000000,").getBytes(StandardCharsets.UTF_8);
		assertThatThrownBy(() -> validate(RecordType.SHIFT_CLOSE, value))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
	}

	@Test
	void shiftExamplesAreCheckedAgainstTheShiftSchema() {
		// businessDate is the date field of a shift close; a summary sent to the shift topic fails its schema
		assertThatThrownBy(() -> validate(RecordType.SHIFT_CLOSE,
				ContractExamples.readShift("invalid-schema/sale-date-instead-of-business-date.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
		assertThatThrownBy(() -> validate(RecordType.SHIFT_CLOSE, ContractExamples.read("valid/basic.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
		// and a shift close sent to the summary or return topic fails those schemas
		assertThatThrownBy(() -> validate(ContractExamples.readShift("valid/basic.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
		assertThatThrownBy(() -> validate(RecordType.DAILY_RETURN, ContractExamples.readShift("valid/basic.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
	}

	@Test
	void identityIsCheckedBeforeBusinessRulesForShiftClose() {
		// Schema error wins over a wrong branch
		assertThatThrownBy(() -> validator.validate(RecordType.SHIFT_CLOSE, "BR0002", "BR0001",
				ContractExamples.readShift("invalid-schema/shift-no-zero.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.SCHEMA_INVALID);
		// A wrong branch or key wins over a business error
		assertThatThrownBy(() -> validator.validate(RecordType.SHIFT_CLOSE, "BR0002", "BR0001",
				ContractExamples.readShift("invalid-business/closed-before-opened.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.BRANCH_MISMATCH);
		assertThatThrownBy(() -> validator.validate(RecordType.SHIFT_CLOSE, "BR0001", "POS01",
				ContractExamples.readShift("invalid-business/duplicate-tender.json")))
				.extracting(e -> ((RejectedMessageException) e).reason()).isEqualTo(RejectReason.KEY_MISMATCH);
	}

	/** Validates as read from the cluster of the branch in the value, key = branchCode. */
	private DailyFigures validate(byte[] value) {
		return validate(RecordType.DAILY_SUMMARY, value);
	}

	/** Validates as read from the topic of {@code type} in the cluster of the branch in the value, key = branchCode. */
	private DailyFigures validate(RecordType type, byte[] value) {
		String branch = ContractExamples.branchCodeOf(value);
		return validator.validate(type, branch, branch, value);
	}

	private void assertRejected(byte[] value, RejectReason reason) {
		assertThatThrownBy(() -> validate(value))
				.isInstanceOf(RejectedMessageException.class)
				.extracting(e -> ((RejectedMessageException) e).reason())
				.isEqualTo(reason);
	}

	/** valid/basic.json with one text replacement; fails if the text is not found. */
	private static byte[] replace(String target, String replacement) {
		String json = new String(ContractExamples.read("valid/basic.json"), StandardCharsets.UTF_8);
		assertThat(json).contains(target);
		return json.replace(target, replacement).getBytes(StandardCharsets.UTF_8);
	}
}
