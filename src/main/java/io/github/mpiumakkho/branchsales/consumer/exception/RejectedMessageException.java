package io.github.mpiumakkho.branchsales.consumer.exception;

import org.jspecify.annotations.Nullable;

import io.github.mpiumakkho.branchsales.consumer.dto.DailySalesSummary;

/**
 * A record that failed a contract check. It is not retried: it is stored in
 * the dead_letter table, the branch gets a REJECTED receipt, and the consumer
 * continues with the next record.
 */
public class RejectedMessageException extends RuntimeException {

	private final RejectReason reason;

	private final @Nullable DailySalesSummary summary;

	/** For records that could not be read as a summary (parse and schema layers). */
	public RejectedMessageException(RejectReason reason, String detail) {
		this(reason, detail, null);
	}

	/** For records that were read but failed a later layer; the receipt then names the day and revision. */
	public RejectedMessageException(RejectReason reason, String detail, @Nullable DailySalesSummary summary) {
		super(reason + ": " + detail);
		this.reason = reason;
		this.summary = summary;
	}

	public RejectReason reason() {
		return reason;
	}

	public @Nullable DailySalesSummary summary() {
		return summary;
	}
}
