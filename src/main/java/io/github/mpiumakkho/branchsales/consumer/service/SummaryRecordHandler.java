package io.github.mpiumakkho.branchsales.consumer.service;

import java.time.OffsetDateTime;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import io.github.mpiumakkho.branchsales.consumer.dto.Receipt;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.repository.DailySalesStore.Outcome;
import io.github.mpiumakkho.branchsales.consumer.repository.DeadLetterStore;

/**
 * Handles one summary record from a branch: stores it, or keeps it in dead_letter if it fails a contract check.
 * Either way it returns the receipt for the branch. Any other failure (HQ database not reachable) is thrown, so the
 * record is retried and no receipt is written.
 */
@Service
public class SummaryRecordHandler {

	private static final Logger log = LoggerFactory.getLogger(SummaryRecordHandler.class);

	private final DailySummaryProcessor processor;
	private final DeadLetterStore deadLetters;
	private final MeterRegistry meters;

	public SummaryRecordHandler(DailySummaryProcessor processor, DeadLetterStore deadLetters, MeterRegistry meters) {
		this.processor = processor;
		this.deadLetters = deadLetters;
		this.meters = meters;
	}

	/**
	 * @param branchCode   the branch whose Kafka cluster the record was read from
	 * @param sourceOffset the record's offset in the branch's summary topic
	 */
	public Receipt handle(String branchCode, long sourceOffset, @Nullable String key, byte @Nullable [] value) {
		try {
			var processed = processor.process(branchCode, key, value);
			var summary = processed.summary();
			var result = processed.result();
			if (result.outcome() == Outcome.STALE) {
				log.warn("{} {}/{} revision {} skipped: stored revision {} is newer (offset {})", result.outcome(),
						summary.branchCode(), summary.saleDate(), summary.revision(), result.storedRevision(),
						sourceOffset);
			}
			else {
				log.info("{} {}/{} revision {} (offset {})", result.outcome(), summary.branchCode(),
						summary.saleDate(), summary.revision(), sourceOffset);
			}
			count(result.outcome().name(), "none");
			return Receipt.stored(branchCode, sourceOffset, summary, result, OffsetDateTime.now());
		}
		catch (RejectedMessageException e) {
			log.warn("Rejected {} offset {}: {} key={}", branchCode, sourceOffset, e.getMessage(), key);
			deadLetters.save(branchCode, sourceOffset, key, value, e);
			count(Receipt.REJECTED, e.reason().name());
			return Receipt.rejected(branchCode, sourceOffset, e, OffsetDateTime.now());
		}
	}

	/** Records handled, by receipt outcome and (for REJECTED) reject reason. Not per branch: dead_letter has that. */
	private void count(String outcome, String reason) {
		Counter.builder("branch_sales.records")
				.description("Summary records handled, by receipt outcome and reject reason")
				.tag("outcome", outcome)
				.tag("reason", reason)
				.register(meters)
				.increment();
	}
}
