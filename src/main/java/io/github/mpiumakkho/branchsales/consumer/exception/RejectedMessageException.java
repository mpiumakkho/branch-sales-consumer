package io.github.mpiumakkho.branchsales.consumer.exception;

/**
 * A record that failed a contract check. It is not retried: it goes to the
 * dead-letter topic and the consumer continues with the next record.
 */
public class RejectedMessageException extends RuntimeException {

	private final RejectReason reason;

	public RejectedMessageException(RejectReason reason, String detail) {
		super(reason + ": " + detail);
		this.reason = reason;
	}

	public RejectReason reason() {
		return reason;
	}
}
