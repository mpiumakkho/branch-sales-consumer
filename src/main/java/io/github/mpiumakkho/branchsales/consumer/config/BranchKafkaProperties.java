package io.github.mpiumakkho.branchsales.consumer.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import io.github.mpiumakkho.branchsales.consumer.dto.RecordType;

/**
 * How HQ connects to the Kafka broker of each branch (requirements §16).
 *
 * @param summaryTopic     topic the branch producer writes daily sales to, in every branch cluster
 * @param returnTopic      topic the branch producer writes daily returns to, in every branch cluster
 * @param shiftCloseTopic  topic the branch producer writes POS shift closes to, in every branch cluster
 * @param receiptTopic     topic HQ writes receipts to, in every branch cluster
 * @param groupId          HQ consumer group in every branch cluster
 * @param securityProtocol SASL_SSL (branches over the WAN) or PLAINTEXT (tests)
 * @param username         HQ's SCRAM user at every branch broker
 * @param passwordDir      one file per branch, named by branch code, holding HQ's password at that branch
 * @param truststore       PEM file with the CA that signed the branch broker certificates
 * @param sendTimeout      how long to wait for a branch broker to acknowledge a receipt
 * @param shard            which branches this instance reads: those whose registry row has this shard. Several
 *                         instances with different shards spread the branches between them
 */
@ConfigurationProperties("branch-sales.branch-kafka")
public record BranchKafkaProperties(
		String summaryTopic,
		String returnTopic,
		String shiftCloseTopic,
		String receiptTopic,
		String groupId,
		String securityProtocol,
		String username,
		@Nullable Path passwordDir,
		@Nullable Path truststore,
		Duration sendTimeout,
		@DefaultValue("default") String shard) {

	private static final Set<String> PROTOCOLS = Set.of("SASL_SSL", "PLAINTEXT");
	// Same rule as infra/onboard-branch.sh
	static final Pattern SHARD = Pattern.compile("^[a-z0-9-]{1,30}$");

	public BranchKafkaProperties {
		if (!SHARD.matcher(shard).matches()) {
			throw new IllegalArgumentException("branch-sales.branch-kafka.shard must match " + SHARD + ", got: " + shard);
		}
		if (!PROTOCOLS.contains(securityProtocol)) {
			throw new IllegalArgumentException("branch-sales.branch-kafka.security-protocol must be one of " + PROTOCOLS);
		}
		if (securityProtocol.equals("SASL_SSL") && (passwordDir == null || truststore == null)) {
			throw new IllegalArgumentException(
					"SASL_SSL needs branch-sales.branch-kafka.password-dir and branch-sales.branch-kafka.truststore");
		}
		// Not Set.of, which throws on duplicates itself
		if (new HashSet<>(List.of(summaryTopic, returnTopic, shiftCloseTopic)).size() != 3) {
			throw new IllegalArgumentException(
					"branch-sales.branch-kafka.summary-topic, return-topic and shift-close-topic must differ");
		}
	}

	/** The topics HQ reads in every branch cluster, one per record type. */
	public String[] recordTopics() {
		return new String[] { summaryTopic, returnTopic, shiftCloseTopic };
	}

	/** The record type of one of {@link #recordTopics()}. */
	public RecordType recordType(String topic) {
		if (topic.equals(summaryTopic)) {
			return RecordType.DAILY_SUMMARY;
		}
		if (topic.equals(returnTopic)) {
			return RecordType.DAILY_RETURN;
		}
		if (topic.equals(shiftCloseTopic)) {
			return RecordType.SHIFT_CLOSE;
		}
		throw new IllegalArgumentException("not a record topic: " + topic);
	}
}
