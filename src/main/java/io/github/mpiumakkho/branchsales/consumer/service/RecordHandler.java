package io.github.mpiumakkho.branchsales.consumer.service;

import java.time.OffsetDateTime;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import io.github.mpiumakkho.branchsales.consumer.dto.Receipt;
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.repository.DailyFiguresStore.Outcome;
import io.github.mpiumakkho.branchsales.consumer.repository.DeadLetterStore;

/**
 * Handles one record from a branch: stores it, or keeps it in dead_letter if it fails a contract check. Either way
 * it returns the receipt for the branch. Any other failure (HQ database not reachable) is thrown, so the record is
 * retried and no receipt is written.
 */
@Service
public class RecordHandler {

	private static final Logger log = LoggerFactory.getLogger(RecordHandler.class);

	private final RecordProcessor processor;
	private final DeadLetterStore deadLetters;

	public RecordHandler(RecordProcessor processor, DeadLetterStore deadLetters) {
		this.processor = processor;
		this.deadLetters = deadLetters;
	}

	/**
	 * @param type         the record type of the topic the record was read from
	 * @param branchCode   the branch whose Kafka cluster the record was read from
	 * @param sourceOffset the record's offset in that topic
	 */
	public Receipt handle(RecordType type, String branchCode, long sourceOffset, @Nullable String key,
			byte @Nullable [] value) {
		try {
			var processed = processor.process(type, branchCode, key, value);
			var figures = processed.figures();
			var result = processed.result();
			if (result.outcome() == Outcome.STALE) {
				log.warn("{} {} {}/{} revision {} skipped: stored revision {} is newer (offset {})", result.outcome(),
						type, figures.branchCode(), figures.date(), figures.revision(), result.storedRevision(),
						sourceOffset);
			}
			else {
				log.info("{} {} {}/{} revision {} (offset {})", result.outcome(), type, figures.branchCode(),
						figures.date(), figures.revision(), sourceOffset);
			}
			return Receipt.stored(type, branchCode, sourceOffset, figures, result, OffsetDateTime.now());
		}
		catch (RejectedMessageException e) {
			log.warn("Rejected {} {} offset {}: {} key={}", type, branchCode, sourceOffset, e.getMessage(), key);
			deadLetters.save(type, branchCode, sourceOffset, key, value, e);
			if (e.reason() == RejectReason.PARENT_MISSING && e.figures() != null
					&& deadLetters.requestReplayIfParentExists(branchCode, sourceOffset, e.figures().date()) > 0) {
				// The sales arrived between the check and the dead_letter write (replayer thread)
				log.info("Sales of {} {} arrived meanwhile: replay of the return at offset {} requested", branchCode,
						e.figures().date(), sourceOffset);
			}
			return Receipt.rejected(type, branchCode, sourceOffset, e, OffsetDateTime.now());
		}
	}
}
