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
import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;
import io.github.mpiumakkho.branchsales.consumer.exception.RejectReason;
import io.github.mpiumakkho.branchsales.consumer.repository.DailyFiguresStore.Outcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Writes receipts to a branch's receipt topic, in that branch's Kafka cluster. Waits for the broker ack, so the
 * caller commits the summary offset only after the branch can read the receipt.
 * <p>
 * Counts the receipts acknowledged ({@code branch_sales.receipts}, by outcome and reject reason): every record HQ
 * has finished with, from a batch or a replay, exactly once per receipt the branch can read.
 */
@Component
public class ReceiptPublisher {

	private static final ZoneId BANGKOK = ZoneId.of("Asia/Bangkok");
	private static final String NO_REASON = "none";

	private final JsonMapper mapper = JsonMapper.builder().build();
	private final BranchKafkaProperties properties;
	private final MeterRegistry meters;

	public ReceiptPublisher(BranchKafkaProperties properties, MeterRegistry meters) {
		this.properties = properties;
		this.meters = meters;
		// Registered up front, so every series exists from the first scrape and rate() works from the first event
		for (RecordType type : RecordType.values()) {
			for (Outcome outcome : Outcome.values()) {
				counter(type, outcome.name(), NO_REASON);
			}
			for (RejectReason reason : RejectReason.values()) {
				counter(type, Receipt.REJECTED, reason.name());
			}
		}
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
		counter(receipt.type(), receipt.outcome(),
				receipt.rejectReason() == null ? NO_REASON : receipt.rejectReason().name()).increment();
	}

	private Counter counter(RecordType type, String outcome, String reason) {
		return Counter.builder("branch_sales.receipts")
				.description("Receipts acknowledged by the branch, by record type, outcome and reject reason")
				.tag("type", type.name())
				.tag("outcome", outcome)
				.tag("reason", reason)
				.register(meters);
	}

	byte[] toJson(Receipt receipt) {
		ObjectNode root = mapper.createObjectNode();
		root.put("schemaVersion", 1);
		root.put("type", receipt.type().name());
		root.put("branchCode", receipt.branchCode());
		root.put("sourceOffset", receipt.sourceOffset());
		if (receipt.eventId() != null) {
			root.put("eventId", receipt.eventId().toString());
		}
		if (receipt.saleDate() != null) {
			root.put("saleDate", receipt.saleDate().toString());
		}
		if (receipt.terminalId() != null) {
			root.put("terminalId", receipt.terminalId());
		}
		if (receipt.shiftNo() != null) {
			root.put("shiftNo", receipt.shiftNo());
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
