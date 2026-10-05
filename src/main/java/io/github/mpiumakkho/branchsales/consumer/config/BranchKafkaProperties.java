package io.github.mpiumakkho.branchsales.consumer.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * How HQ connects to the Kafka broker of each branch (requirements §16).
 *
 * @param summaryTopic     topic the branch producer writes, in every branch cluster
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
	}
}
