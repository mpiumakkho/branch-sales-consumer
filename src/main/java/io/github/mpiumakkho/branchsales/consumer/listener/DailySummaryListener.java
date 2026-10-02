package io.github.mpiumakkho.branchsales.consumer.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.repository.DailySalesStore.Outcome;
import io.github.mpiumakkho.branchsales.consumer.service.DailySummaryProcessor;

/**
 * Batch listener for the branch topics {@code branch-sales.daily-summary.<branchCode>}.
 * <p>
 * Offsets are committed by the container after this method returns (ack mode
 * BATCH), which is after every record's database transaction has committed.
 * A record that fails a contract check goes to the dead-letter topic and the
 * batch continues. Any other failure (database or Kafka down) stops the batch
 * at that record; the error handler commits the records before it and retries
 * from it.
 */
@Component
public class DailySummaryListener {

	private static final Logger log = LoggerFactory.getLogger(DailySummaryListener.class);

	private final DailySummaryProcessor processor;
	private final DeadLetterPublishingRecoverer deadLetters;

	public DailySummaryListener(DailySummaryProcessor processor, DeadLetterPublishingRecoverer deadLetters) {
		this.processor = processor;
		this.deadLetters = deadLetters;
	}

	// One topic per branch; topics of newly onboarded branches are picked up at the next metadata refresh
	@KafkaListener(topicPattern = "${branch-sales.kafka.topic-pattern}")
	public void onBatch(List<ConsumerRecord<String, byte[]>> records) {
		for (int i = 0; i < records.size(); i++) {
			ConsumerRecord<String, byte[]> record = records.get(i);
			try {
				handle(record);
			}
			catch (RuntimeException e) {
				throw new BatchListenerFailedException("failed at " + position(record), e, i);
			}
		}
	}

	private void handle(ConsumerRecord<String, byte[]> record) {
		try {
			var processed = processor.process(record.topic(), record.key(), record.value());
			var summary = processed.summary();
			var result = processed.result();
			if (result.outcome() == Outcome.STALE) {
				log.warn("{} {}/{} revision {} skipped: stored revision {} is newer ({})", result.outcome(),
						summary.branchCode(), summary.saleDate(), summary.revision(), result.storedRevision(),
						position(record));
			}
			else {
				log.info("{} {}/{} revision {} ({})", result.outcome(), summary.branchCode(), summary.saleDate(),
						summary.revision(), position(record));
			}
		}
		catch (RejectedMessageException e) {
			log.warn("Rejected to dead-letter topic: {} key={} ({})", e.getMessage(), record.key(), position(record));
			// Waits for the broker ack; throws if the dead-letter record could not be written
			deadLetters.accept(record, e);
		}
	}

	private static String position(ConsumerRecord<?, ?> record) {
		return record.topic() + "-" + record.partition() + "@" + record.offset();
	}
}
