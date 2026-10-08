package io.github.mpiumakkho.branchsales.consumer.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.mpiumakkho.branchsales.consumer.dto.DailyFigures;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.repository.DailyFiguresStore;
import io.github.mpiumakkho.branchsales.consumer.repository.DeadLetterStore;

/**
 * Validates one record value and stores it. Each record gets its own database transaction, so a rejected record
 * does not roll back the others in the batch. Storing a day's sales also asks for the replay of that day's returns
 * that were rejected with PARENT_MISSING, in the same transaction.
 */
@Service
public class RecordProcessor {

	private final RecordValidator validator;
	private final ReferenceChecker referenceChecker;
	private final DailyFiguresStore store;
	private final DeadLetterStore deadLetters;
	private final TransactionTemplate transaction;

	public RecordProcessor(RecordValidator validator, ReferenceChecker referenceChecker, DailyFiguresStore store,
			DeadLetterStore deadLetters, TransactionTemplate transaction) {
		this.validator = validator;
		this.referenceChecker = referenceChecker;
		this.store = store;
		this.deadLetters = deadLetters;
		this.transaction = transaction;
	}

	public record Processed(DailyFigures figures, DailyFiguresStore.Result result) {
	}

	/**
	 * @param type       the record type of the topic the record was read from
	 * @param branchCode the branch whose Kafka cluster the record was read from
	 * @return the stored outcome; the transaction is committed when this returns
	 * @throws RejectedMessageException if the value fails a contract check
	 */
	public Processed process(RecordType type, String branchCode, String key, byte[] value) {
		DailyFigures figures = validator.validate(type, branchCode, key, value);
		DailyFiguresStore.Result result = transaction.execute(status -> {
			referenceChecker.check(figures);
			DailyFiguresStore.Result stored = store.apply(figures);
			if (type == RecordType.DAILY_SUMMARY) {
				deadLetters.requestReplayOfReturnsWaitingFor(figures.branchCode(), figures.date());
			}
			return stored;
		});
		return new Processed(figures, result);
	}
}
