package io.github.mpiumakkho.branchsales.consumer.scheduler;

import java.time.OffsetDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.dto.Receipt;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectedMessageException;
import io.github.mpiumakkho.branchsales.consumer.kafka.ReceiptPublisher;
import io.github.mpiumakkho.branchsales.consumer.listener.BranchListeners;
import io.github.mpiumakkho.branchsales.consumer.repository.DeadLetterStore;
import io.github.mpiumakkho.branchsales.consumer.repository.DeadLetterStore.DeadLetter;
import io.github.mpiumakkho.branchsales.consumer.service.RecordProcessor;

/**
 * Processes dead letters again when an HQ admin asks for it ({@code update dead_letter set replay_requested_at = now()
 * where ...}), for example after adding a missing category. Requirements Q8.
 * <p>
 * The record goes through all contract checks again and the branch gets a new receipt for the same source offset.
 * A row is marked as replayed only after the branch acknowledged the receipt; if the receipt fails, the row is tried
 * again at the next run. Only rows of branches this instance is connected to are taken, so with several consumer
 * instances the one that reads the branch does the replay. Processing it again is safe: a stored record then gives
 * DUPLICATE.
 */
@Component
public class DeadLetterReplayer {

	private static final Logger log = LoggerFactory.getLogger(DeadLetterReplayer.class);

	private static final int BATCH_SIZE = 100;

	private final DeadLetterStore deadLetters;
	private final RecordProcessor processor;
	private final BranchListeners branches;
	private final ReceiptPublisher receipts;

	public DeadLetterReplayer(DeadLetterStore deadLetters, RecordProcessor processor, BranchListeners branches,
			ReceiptPublisher receipts) {
		this.deadLetters = deadLetters;
		this.processor = processor;
		this.branches = branches;
		this.receipts = receipts;
	}

	@Scheduled(fixedDelayString = "${branch-sales.dead-letter.replay-interval-ms}")
	public void replayRequested() {
		for (DeadLetter deadLetter : deadLetters.findReplayRequested(BATCH_SIZE, branches.connectedBranches())) {
			var branchKafka = branches.receipts(deadLetter.branchCode());
			if (branchKafka.isEmpty()) {
				log.warn("Replay of dead letter {} waits: branch {} is not connected", deadLetter.id(),
						deadLetter.branchCode());
				continue;
			}
			try {
				replay(deadLetter, branchKafka.get());
			}
			catch (RuntimeException e) {
				log.warn("Replay of dead letter {} failed, tried again at the next run: {}", deadLetter.id(),
						e.getMessage());
			}
		}
	}

	private void replay(DeadLetter deadLetter, KafkaTemplate<String, byte[]> branchKafka) {
		String branchCode = deadLetter.branchCode();
		long offset = deadLetter.sourceOffset();
		try {
			var processed = processor.process(deadLetter.type(), branchCode, deadLetter.recordKey(),
					deadLetter.recordValue());
			receipts.publish(branchKafka, Receipt.stored(deadLetter.type(), branchCode, offset, processed.figures(),
					processed.result(), OffsetDateTime.now()));
			deadLetters.markReplayed(deadLetter.id(), processed.result().outcome().name());
			log.info("Replayed dead letter {} ({} {} offset {}): {}", deadLetter.id(), deadLetter.type(), branchCode,
					offset, processed.result().outcome());
		}
		catch (RejectedMessageException e) {
			receipts.publish(branchKafka, Receipt.rejected(deadLetter.type(), branchCode, offset, e, OffsetDateTime.now()));
			deadLetters.markReplayRejected(deadLetter.id(), e);
			log.warn("Replayed dead letter {} ({} offset {}) rejected again: {}", deadLetter.id(), branchCode, offset,
					e.getMessage());
		}
	}
}
