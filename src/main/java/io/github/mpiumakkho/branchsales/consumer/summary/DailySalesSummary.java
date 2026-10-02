package io.github.mpiumakkho.branchsales.consumer.summary;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * A {@code DailySalesSummary} message (contract v1) that passed the schema check.
 * Only the fields HQ uses are kept; unknown fields in the message are ignored.
 */
public record DailySalesSummary(
		UUID eventId,
		String branchCode,
		LocalDate saleDate,
		int revision,
		OffsetDateTime confirmedAt,
		BigDecimal totalAmount,
		List<SalesLine> lines) {

	public DailySalesSummary {
		lines = List.copyOf(lines);
	}

	public record SalesLine(String categoryCode, BigDecimal amount, long quantity) {
	}
}
