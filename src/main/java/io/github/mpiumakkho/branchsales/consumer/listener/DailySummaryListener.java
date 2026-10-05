package io.github.mpiumakkho.branchsales.consumer.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.dto.Receipt;
import io.github.mpiumakkho.branchsales.consumer.kafka.ReceiptPublisher;
import io.github.mpiumakkho.branchsales.consumer.service.SummaryRecordHandler;

/**
 * Batch listener for one branch's summary topic. {@link BranchListeners} starts one container per branch.
 * <p>
 * For each record: store it (or keep it in dead_letter if it fails a contract check), then write the receipt to the
 * branch and wait for the ack. Offsets are committed by the container after this method returns (ack mode BATCH), so
 * after every record's database commit and receipt. Any other failure (HQ database or branch Kafka not reachable)
 * stops the batch at that record; the error handler commits the records before it and retries from it.
 */
@Component
public class DailySummaryListener {

	private final SummaryRecordHandler handler;
	private final ReceiptPublisher receipts;

	public DailySummaryListener(SummaryRecordHandler handler, ReceiptPublisher receipts) {
		this.handler = handler;
		this.receipts = receipts;
	}

	public void onBatch(String branchCode, KafkaTemplate<String, byte[]> branchKafka,
			List<ConsumerRecord<String, byte[]>> records) {
		for (int i = 0; i < records.size(); i++) {
			ConsumerRecord<String, byte[]> record = records.get(i);
			try {
				Receipt receipt = handler.handle(branchCode, record.offset(), record.key(), record.value());
				receipts.publish(branchKafka, receipt);
			}
			catch (RuntimeException e) {
				throw new BatchListenerFailedException(
						"failed at " + branchCode + " " + record.topic() + "@" + record.offset(), e, i);
			}
		}
	}
}
