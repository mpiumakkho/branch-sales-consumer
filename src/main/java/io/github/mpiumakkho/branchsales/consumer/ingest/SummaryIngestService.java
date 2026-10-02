package io.github.mpiumakkho.branchsales.consumer.ingest;

import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import io.github.mpiumakkho.branchsales.consumer.store.DailySalesStore;
import io.github.mpiumakkho.branchsales.consumer.summary.DailySalesSummary;
import io.github.mpiumakkho.branchsales.consumer.validation.ReferenceChecker;
import io.github.mpiumakkho.branchsales.consumer.validation.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.validation.SummaryValidator;

/**
 * Validates one record value and stores it. Each record gets its own database
 * transaction, so a rejected record does not roll back the others in the batch.
 */
@Service
public class SummaryIngestService {

	private final SummaryValidator validator;
	private final ReferenceChecker referenceChecker;
	private final DailySalesStore store;
	private final TransactionTemplate transaction;

	public SummaryIngestService(SummaryValidator validator, ReferenceChecker referenceChecker, DailySalesStore store,
			TransactionTemplate transaction) {
		this.validator = validator;
		this.referenceChecker = referenceChecker;
		this.store = store;
		this.transaction = transaction;
	}

	public record Ingested(DailySalesSummary summary, DailySalesStore.Result result) {
	}

	/**
	 * @return the stored outcome; the transaction is committed when this returns
	 * @throws RejectedMessageException if the value fails a contract check
	 */
	public Ingested ingest(byte[] value) {
		DailySalesSummary summary = validator.validate(value);
		DailySalesStore.Result result = transaction.execute(status -> {
			referenceChecker.check(summary);
			return store.apply(summary);
		});
		return new Ingested(summary, result);
	}
}
