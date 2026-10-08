package io.github.mpiumakkho.branchsales.consumer.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * A branch record that passed the schema check: one business day of one branch by category, with a revision. A
 * {@code DailySalesSummary} or a {@code DailyReturn} (contract v1), told apart by {@link #type}. Only the fields HQ
 * uses are kept; unknown fields in the message are ignored.
 *
 * @param date the business date: {@code saleDate} of a summary, {@code returnDate} of a return
 */
public record DailyFigures(
		RecordType type,
		UUID eventId,
		String branchCode,
		LocalDate date,
		int revision,
		OffsetDateTime confirmedAt,
		BigDecimal totalAmount,
		List<Line> lines) {

	public DailyFigures {
		lines = List.copyOf(lines);
	}

	public record Line(String categoryCode, BigDecimal amount, long quantity) {
	}
}
