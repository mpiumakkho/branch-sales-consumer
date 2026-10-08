package io.github.mpiumakkho.branchsales.consumer.dto;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

import org.jspecify.annotations.Nullable;

import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.repository.DailyFiguresStore;

/**
 * A {@code DailySalesReceipt} (contract/daily-sales-receipt.v1.schema.json): what HQ did with one record of a
 * branch, a summary or a return.
 *
 * @param type    the record's type: the receipt is matched by (type, sourceOffset), since offsets are per topic
 * @param outcome INSERTED, UPDATED, DUPLICATE, STALE or REJECTED
 */
public record Receipt(
		RecordType type,
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

	public static Receipt stored(RecordType type, String branchCode, long sourceOffset, DailyFigures figures,
			DailyFiguresStore.Result result, OffsetDateTime processedAt) {
		return new Receipt(type, branchCode, sourceOffset, figures.eventId(), figures.date(), figures.revision(),
				result.outcome().name(), result.storedRevision(), null, null, processedAt);
	}

	public static Receipt rejected(RecordType type, String branchCode, long sourceOffset,
			RejectedMessageException rejection, OffsetDateTime processedAt) {
		DailyFigures figures = rejection.figures();
		return new Receipt(type, branchCode, sourceOffset,
				figures == null ? null : figures.eventId(),
				figures == null ? null : figures.date(),
				figures == null ? null : figures.revision(),
				REJECTED, null, rejection.reason(), truncate(rejection.getMessage()), processedAt);
	}

	private static String truncate(String detail) {
		return detail.length() > MAX_DETAIL_LENGTH ? detail.substring(0, MAX_DETAIL_LENGTH) : detail;
	}
}
