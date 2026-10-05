package io.github.mpiumakkho.branchsales.consumer.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.stereotype.Component;

import io.github.mpiumakkho.branchsales.consumer.config.BranchKafkaProperties;

/**
 * Client settings for one branch's Kafka cluster: the shared settings from {@code spring.kafka.consumer.*} and
 * {@code spring.kafka.producer.*}, plus that branch's address and HQ's credentials there.
 */
@Component
public class BranchKafkaClients {

	// Same characters onboard-branch.sh allows: nothing that needs quoting in the JAAS line
	private static final Pattern PASSWORD = Pattern.compile("^[A-Za-z0-9._~-]{8,128}$");

	private final KafkaProperties kafka;
	private final BranchKafkaProperties branchKafka;

	public BranchKafkaClients(KafkaProperties kafka, BranchKafkaProperties branchKafka) {
		this.kafka = kafka;
		this.branchKafka = branchKafka;
	}

	/** @throws IllegalStateException if HQ's password for this branch is missing or invalid */
	public Map<String, Object> consumerConfig(String branchCode, String bootstrap) {
		Map<String, Object> config = new HashMap<>(kafka.buildConsumerProperties());
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, branchKafka.groupId());
		config.put(ConsumerConfig.CLIENT_ID_CONFIG, "hq-consumer-" + branchCode);
		addSecurity(config, branchCode);
		return config;
	}

	/** @throws IllegalStateException if HQ's password for this branch is missing or invalid */
	public Map<String, Object> producerConfig(String branchCode, String bootstrap) {
		Map<String, Object> config = new HashMap<>(kafka.buildProducerProperties());
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
		config.put(ProducerConfig.CLIENT_ID_CONFIG, "hq-receipts-" + branchCode);
		addSecurity(config, branchCode);
		return config;
	}

	private void addSecurity(Map<String, Object> config, String branchCode) {
		config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, branchKafka.securityProtocol());
		if (!branchKafka.securityProtocol().equals("SASL_SSL")) {
			return;
		}
		config.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
		config.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required"
				+ " username=\"" + branchKafka.username() + "\" password=\"" + password(branchCode) + "\";");
		// The broker certificate must be for the host in the registered address (kafka.<branch>.example), signed by
		// the HQ CA. Hostname verification stays on (client default "https").
		config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
		config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, branchKafka.truststore().toString());
	}

	private String password(String branchCode) {
		Path file = branchKafka.passwordDir().resolve(branchCode);
		String password;
		try {
			password = Files.readString(file, StandardCharsets.UTF_8).strip();
		}
		catch (IOException e) {
			throw new IllegalStateException("cannot read HQ's Kafka password for branch " + branchCode + " from " + file, e);
		}
		if (!PASSWORD.matcher(password).matches()) {
			throw new IllegalStateException("HQ's Kafka password for branch " + branchCode + " in " + file
					+ " must be 8-128 characters from A-Z a-z 0-9 . _ ~ -");
		}
		return password;
	}
}
