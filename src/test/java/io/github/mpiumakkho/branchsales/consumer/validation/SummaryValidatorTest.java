package io.github.mpiumakkho.branchsales.consumer.validation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
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
import io.github.mpiumakkho.branchsales.consumer.summary.DailySalesSummary;
import io.github.mpiumakkho.branchsales.consumer.summary.DailySalesSummary.SalesLine;

class SummaryValidatorTest {

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

	private final SummaryValidator validator = new SummaryValidator();

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
			assertThat(validator.validate(value)).isNotNull();
		}
		else {
			assertRejected(value, REJECTED.get(example));
		}
	}

	@Test
	void mapsFieldsWithMoneyAsBigDecimal() {
		DailySalesSummary summary = validator.validate(ContractExamples.read("valid/basic.json"));

		assertThat(summary.eventId()).isEqualTo(UUID.fromString("3f1c2a9e-8b4d-4c1e-9f2a-6d7e8a9b0c1d"));
		assertThat(summary.branchCode()).isEqualTo("BR0001");
		assertThat(summary.saleDate()).isEqualTo(LocalDate.of(2026, 10, 1));
		assertThat(summary.revision()).isEqualTo(1);
		assertThat(summary.confirmedAt().toString()).isEqualTo("2026-10-01T21:45+07:00");
		assertThat(summary.totalAmount()).isEqualTo(new BigDecimal("41870.50"));
		assertThat(summary.lines()).hasSize(4)
				.first().isEqualTo(new SalesLine("BEVERAGE", new BigDecimal("18200.00"), 410));
	}

	@ParameterizedTest
	@ValueSource(strings = { "not json", "{\"schemaVersion\": 1", "{} {}" })
	void rejectsValueThatIsNotJson(String value) {
		assertRejected(value.getBytes(StandardCharsets.UTF_8), RejectReason.INVALID_JSON);
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

	private void assertRejected(byte[] value, RejectReason reason) {
		assertThatThrownBy(() -> validator.validate(value))
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
