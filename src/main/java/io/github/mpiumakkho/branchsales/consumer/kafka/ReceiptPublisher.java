package io.github.mpiumakkho.branchsales.consumer.kafka;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

import io.github.mpiumakkho.branchsales.consumer.config.BranchKafkaProperties;
import io.github.mpiumakkho.branchsales.consumer.dto.Receipt;

/**
 * Writes receipts to a branch's receipt topic, in that branch's Kafka cluster. Waits for the broker ack, so the
 * caller commits the summary offset only after the branch can read the receipt.
 */
@Component
public class ReceiptPublisher {

	private static final ZoneId BANGKOK = ZoneId.of("Asia/Bangkok");

	private final JsonMapper mapper = JsonMapper.builder().build();
	private final BranchKafkaProperties properties;

	public ReceiptPublisher(BranchKafkaProperties properties) {
		this.properties = properties;
	}

	/** @throws ReceiptNotSentException if the branch broker did not acknowledge the receipt in time */
	public void publish(KafkaTemplate<String, byte[]> branchKafka, Receipt receipt) {
		try {
			branchKafka.send(properties.receiptTopic(), receipt.branchCode(), toJson(receipt))
					.get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ReceiptNotSentException(receipt, e);
		}
		catch (ExecutionException | TimeoutException | RuntimeException e) {
			throw new ReceiptNotSentException(receipt, e);
		}
	}

	byte[] toJson(Receipt receipt) {
		ObjectNode root = mapper.createObjectNode();
		root.put("schemaVersion", 1);
		root.put("branchCode", receipt.branchCode());
		root.put("sourceOffset", receipt.sourceOffset());
		if (receipt.eventId() != null) {
			root.put("eventId", receipt.eventId().toString());
		}
		if (receipt.saleDate() != null) {
			root.put("saleDate", receipt.saleDate().toString());
		}
		if (receipt.revision() != null) {
			root.put("revision", receipt.revision());
		}
		root.put("outcome", receipt.outcome());
		if (receipt.storedRevision() != null) {
			root.put("storedRevision", receipt.storedRevision());
		}
		if (receipt.rejectReason() != null) {
			root.put("rejectReason", receipt.rejectReason().name());
		}
		if (receipt.detail() != null) {
			root.put("detail", receipt.detail());
		}
		root.put("processedAt",
				DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(receipt.processedAt().atZoneSameInstant(BANGKOK)));
		return mapper.writeValueAsBytes(root);
	}

	public static class ReceiptNotSentException extends RuntimeException {

		ReceiptNotSentException(Receipt receipt, Exception cause) {
			super("receipt for " + receipt.branchCode() + " offset " + receipt.sourceOffset()
					+ " not acknowledged by the branch Kafka", cause);
		}
	}
}
