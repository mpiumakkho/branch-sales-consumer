package io.github.mpiumakkho.branchsales.consumer.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

/**
 * A branch record that passed the schema check: the key of the record, a revision, a total and lines, told apart by
 * {@link #type}. Only the fields HQ uses are kept; unknown fields in the message are ignored.
 *
 * @param key   the record's key within the branch: the business date, plus terminal and shift for a shift close
 * @param lines by {@link RecordType.LineShape#codeField()} of the type
 * @param shift the header fields of a shift close; null for the other types
 */
public record DailyFigures(
		RecordType type,
		UUID eventId,
		String branchCode,
		RecordKey key,
		int revision,
		OffsetDateTime confirmedAt,
		BigDecimal totalAmount,
		List<Line> lines,
		@Nullable ShiftDetail shift) {

	public DailyFigures {
		lines = List.copyOf(lines);
	}

	/** The business date: {@code saleDate} of a summary, {@code returnDate} of a return. */
	public LocalDate date() {
		return key.date();
	}

	/** @param code the line's categoryCode or tenderType, by the type's {@link RecordType.LineShape} */
	public record Line(String code, BigDecimal amount, long quantity) {
	}

	/** The fields of a shift close (contract {@code ShiftClose}) that only that type has. */
	public record ShiftDetail(
			@Nullable String cashierId,
			OffsetDateTime openedAt,
			OffsetDateTime closedAt,
			long transactionCount,
			BigDecimal cashExpected,
			BigDecimal cashCounted) {
	}
}
