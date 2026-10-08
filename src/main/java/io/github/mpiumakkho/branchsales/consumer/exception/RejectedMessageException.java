package io.github.mpiumakkho.branchsales.consumer.exception;

import org.jspecify.annotations.Nullable;

import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;

/**
 * A record that failed a contract check. It is not retried: it is stored in
 * the dead_letter table, the branch gets a REJECTED receipt, and the consumer
 * continues with the next record.
 */
public class RejectedMessageException extends RuntimeException {

	private final RejectReason reason;

	private final @Nullable DailyFigures figures;

	/** For records that could not be read (parse and schema layers). */
	public RejectedMessageException(RejectReason reason, String detail) {
		this(reason, detail, null);
	}

	/** For records that were read but failed a later layer; the receipt then names the day and revision. */
	public RejectedMessageException(RejectReason reason, String detail, @Nullable DailyFigures figures) {
		super(reason + ": " + detail);
		this.reason = reason;
		this.figures = figures;
	}

	public RejectReason reason() {
		return reason;
	}

	public @Nullable DailyFigures figures() {
		return figures;
	}
}
