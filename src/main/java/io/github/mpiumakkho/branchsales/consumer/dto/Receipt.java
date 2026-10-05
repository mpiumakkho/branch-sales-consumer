package io.github.mpiumakkho.branchsales.consumer.dto;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.repository.DailySalesStore;

/**
 * A {@code DailySalesReceipt} (contract/daily-sales-receipt.v1.schema.json): what HQ did with one summary record.
 *
 * @param outcome INSERTED, UPDATED, DUPLICATE, STALE or REJECTED
 */
public record Receipt(
		String branchCode,
		long sourceOffset,
		@Nullable UUID eventId,
		@Nullable LocalDate saleDate,
		@Nullable Integer revision,
		String outcome,
		@Nullable Integer storedRevision,
		@Nullable RejectReason rejectReason,
		@Nullable String detail,
		OffsetDateTime processedAt) {

	public static final String REJECTED = "REJECTED";

	/** Contract limit of {@code detail}. */
	static final int MAX_DETAIL_LENGTH = 2000;

	public static Receipt stored(String branchCode, long sourceOffset, DailySalesSummary summary,
			DailySalesStore.Result result, OffsetDateTime processedAt) {
		return new Receipt(branchCode, sourceOffset, summary.eventId(), summary.saleDate(), summary.revision(),
				result.outcome().name(), result.storedRevision(), null, null, processedAt);
	}

	public static Receipt rejected(String branchCode, long sourceOffset, RejectedMessageException rejection,
			OffsetDateTime processedAt) {
		DailySalesSummary summary = rejection.summary();
		return new Receipt(branchCode, sourceOffset,
				summary == null ? null : summary.eventId(),
				summary == null ? null : summary.saleDate(),
				summary == null ? null : summary.revision(),
				REJECTED, null, rejection.reason(), truncate(rejection.getMessage()), processedAt);
	}

	private static String truncate(String detail) {
		return detail.length() > MAX_DETAIL_LENGTH ? detail.substring(0, MAX_DETAIL_LENGTH) : detail;
	}
}
